from datetime import datetime, timedelta, timezone
from time import sleep, time
import requests
from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Union
from airbyte_cdk.models import AirbyteStream, SyncMode
from airbyte_cdk.sources.streams.core import CheckpointMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
import logging

from .authenticator import GainsightCsAuthenticator
import json
class GainsightCsStream(HttpStream, ABC):
    def __init__(self, authenticator: 'GainsightCsAuthenticator', **kwargs):
        super().__init__(**kwargs)
        self._authenticator = authenticator

    @property
    def authenticator(self):
        return self._authenticator
    
    @property
    def url_base(self):
        return f"{self._authenticator.domain_url}/v1/"


class GainsightCsObjectStream(GainsightCsStream, CheckpointMixin):
    limit = 5000
    SLICE_RANGE_DAYS = 30
    json_schema = None
    raise_on_http_errors = False
    # Tells the Airbyte CDK to emit an intermediate STATE message every 5,000 records.
    state_checkpoint_interval = 5000

    gainsight_airbyte_type_map = {
        "STRING": ["null", "string"],
        "BOOLEAN": ["null", "boolean"],
        "NUMBER": ["null", "number"],
        "PERCENTAGE": ["null", "number"],
        "CURRENCY": ["null", "number"],
        "GSID": ["null", "string"],
        "SFDCID": ["null", "string"],
        "EMAIL": ["null", "string"],
        "URL": ["null", "string"],
        "RICHTEXTAREA": ["null", "string"],
        "LOOKUP": ["null", "string"],
        "JSON": ["null", "object"],
        "JSONBOOLEAN": ["null", "boolean"],
        "JSONNUMBER": ["null", "number"],
        "JSONSTRING": ["null", "string"]
    }

    def __init__(self, name: str, authenticator: GainsightCsAuthenticator, **kwargs):
        super().__init__(authenticator, **kwargs)
        self.object_name = name
        self._primary_key = None
        self.offset = 0
        self._cursor_field_override: Optional[str] = None
        self._current_slice: Optional[Mapping[str, Any]] = None
        self._state: MutableMapping[str, Any] = {}

    @property
    def state(self) -> MutableMapping[str, Any]:
        """State getter so AbstractSource can set state before read(); CDK then uses this in Stream.read()."""
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        """State setter so incoming stream state from the state file is stored and passed to stream_slices."""
        if value is None:
            self._state = {}
        elif isinstance(value, dict):
            self._state = dict(value)
        else:
            self._state = {}

    def _get_checkpoint_reader(self, logger, cursor_field, sync_mode, stream_state):
        """Override to set _cursor_field_override from the catalog cursor_field *before*
        _checkpoint_mode is evaluated. Without this, cursor_field returns [] and CDK 0.90
        routes the stream to RESUMABLE_FULL_REFRESH, which re-calls stream_slices with
        updated state after every STATE emission — causing an infinite sync loop."""
        if cursor_field and len(cursor_field) > 0:
            self._cursor_field_override = cursor_field[0]
        else:
            self._cursor_field_override = None
        return super()._get_checkpoint_reader(
            logger=logger, cursor_field=cursor_field, sync_mode=sync_mode, stream_state=stream_state
        )

    def as_airbyte_stream(self) -> AirbyteStream:
        """Expose both full_refresh and incremental sync modes with no source-defined cursor.
        The user selects a cursor field in the catalog for any stream they want to sync incrementally.
        """
        stream = AirbyteStream(
            name=self.name,
            json_schema=dict(self.get_json_schema()),
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
            source_defined_cursor=False,
        )

        if self.namespace:
            stream.namespace = self.namespace

        keys = Stream._wrapped_primary_key(self.primary_key)
        if keys and len(keys) > 0:
            stream.source_defined_primary_key = keys

        return stream

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        """Return the catalog-configured cursor field so CDK routes to INCREMENTAL (not RESUMABLE_FULL_REFRESH)."""
        if self._cursor_field_override:
            return self._cursor_field_override
        return []

    def _effective_cursor_field(self) -> Optional[str]:
        """Returns the catalog-configured cursor field when set by read_records, else None."""
        return self._cursor_field_override

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
        **kwargs,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        # Use catalog-configured cursor when provided (incremental with custom cursor); else no cursor
        if cursor_field and len(cursor_field) > 0:
            cursor_name = cursor_field[0]
        else:
            cursor_name = None

        # No cursor field or full refresh: single slice, no time bounds (existing full-scan behavior)
        if not cursor_name or sync_mode != SyncMode.incremental:
            self.logger.debug("Stream '%s': no cursor or full-refresh mode, yielding single full-scan slice", self.object_name)
            yield {}
            return

        # Only apply time-window slicing to date/date-time cursor fields.
        # Non-datetime cursors (e.g. numeric Id, string) fall back to single GTE slice.
        schema_properties = self.get_json_schema().get("properties", {})
        cursor_format = schema_properties.get(cursor_name, {}).get("format")
        if cursor_format not in ("date", "date-time"):
            self.logger.info(
                "Stream '%s': cursor '%s' is not a date/date-time field, yielding single GTE slice",
                self.object_name, cursor_name,
            )
            yield {}
            return

        start_str = (stream_state or {}).get(cursor_name)

        # First sync with no prior state: full scan, one STATE checkpoint at end
        if not start_str:
            self.logger.info(
                "Stream '%s': incremental with cursor '%s', no prior state — full scan (single slice)",
                self.object_name, cursor_name,
            )
            yield {}
            return

        try:
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            self.logger.info(
                "Stream '%s': could not parse state for cursor '%s' (value: %s), yielding single full-scan slice",
                self.object_name, cursor_name, start_str,
            )
            yield {}
            return

        now_dt = datetime.now(tz=timezone.utc)
        delta = timedelta(days=self.SLICE_RANGE_DAYS)
        slice_start = start_dt

        while slice_start < now_dt:
            slice_end = min(slice_start + delta, now_dt)
            if cursor_format == "date":
                start_fmt = slice_start.strftime("%Y-%m-%d")
                end_fmt = slice_end.strftime("%Y-%m-%d")
            else:
                start_fmt = slice_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                end_fmt = slice_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            self.logger.info(
                "Stream '%s': yielding slice %s -> %s on cursor '%s'",
                self.object_name, start_fmt, end_fmt, cursor_name,
            )
            yield {
                "cursor_field": cursor_name,
                "cursor_format": cursor_format,
                "start": start_fmt,
                "end": end_fmt,
            }
            slice_start = slice_end

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
        **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        # Use configured cursor field (e.g. ["Date"]) when provided so streams can use different cursors
        if cursor_field and len(cursor_field) > 0:
            self._cursor_field_override = cursor_field[0]
        else:
            self._cursor_field_override = None
        # Reset offset to 0 for each slice so pagination always starts from the beginning of the slice
        self.offset = 0
        self._current_slice = stream_slice
        if stream_slice:
            self.logger.info(
                "Stream '%s': reading slice %s -> %s (cursor: '%s', offset reset to 0)",
                self.object_name,
                stream_slice.get("start", "unbounded"),
                stream_slice.get("end", "unbounded"),
                stream_slice.get("cursor_field", "none"),
            )
        try:
            yield from super().read_records(sync_mode, cursor_field, stream_slice, stream_state, **kwargs)
        finally:
            # Advance self._state to at least the slice end so empty slices still move state forward.
            # CDK 0.90 reads stream.state via _observe_state() after each slice, so this ensures
            # slices with no records still advance the checkpoint rather than stalling.
            cursor = self._cursor_field_override
            slice_end = (self._current_slice or {}).get("end", "")
            if cursor and slice_end:
                current = self._state.get(cursor, "")
                if slice_end > current:
                    self._state = {cursor: slice_end}
            self._cursor_field_override = None
            self._current_slice = None

    @property
    def name(self):
        return self.object_name

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self._primary_key

    def dynamic_schema(self, full_schema, metadata):
        logger = logging.getLogger(__name__)
        is_scorecard_object = "scorecard" in self.object_name.lower()
        
        for field in metadata:
            field_name = field['fieldName']
            data_type = field['dataType']

            if data_type == "DATE":
                full_schema['properties'][field_name] = {"type": ["null", "string"], "format": "date"}
                continue
            if data_type == "DATETIME":
                full_schema['properties'][field_name] = {"type": ["null", "string"], "format": "date-time", "airbyte_type": "timestamp_with_timezone"}
                continue

            field_type = self.gainsight_airbyte_type_map.get(data_type, ["null", "string"])
            
            if data_type not in self.gainsight_airbyte_type_map:
                logger.warning(f"Object '{self.object_name}' field '{field_name}' with dataType '{data_type}' not found in type mapping, defaulting to ['null', 'string']")
            
            # Override field Id to be number type for scorecard objects
            if is_scorecard_object and field_name == "Id":
                field_type = ["null", "number"]
                logger.info(f"Object '{self.object_name}' field 'Id' overridden to ['null', 'number'] for scorecard object")
            
            full_schema['properties'][field_name] = {
                "type": field_type
            }
        return full_schema

    @property
    def http_method(self) -> str:
        return "POST"

    def request_body_json(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Union[Dict[str, Any], str]]:
        logger = logging.getLogger(__name__)
        select_columns = self.get_select_columns()
        offset = self.offset if next_page_token is None else next_page_token
        body = {
            "select": select_columns,
            "limit": self.limit,
            "offset": offset,
        }

        slice_start = (stream_slice or {}).get("start")
        slice_end = (stream_slice or {}).get("end")
        slice_cursor = (stream_slice or {}).get("cursor_field")
        cursor_field_name = slice_cursor or self._effective_cursor_field()
        schema_properties = self.get_json_schema().get("properties", {})

        if slice_start and slice_end and cursor_field_name:
            # Time-window slice: bounded GTE + LT filter with orderBy for deterministic offset pagination
            if cursor_field_name in schema_properties:
                body["where"] = {
                    "conditions": [
                        {"name": cursor_field_name, "alias": "A", "value": [slice_start], "operator": "GTE"},
                        {"name": cursor_field_name, "alias": "B", "value": [slice_end], "operator": "LT"},
                    ],
                    "expression": "A AND B",
                }
                body["orderBy"] = {cursor_field_name: "asc"}
            else:
                logger.warning(
                    "Cursor field '%s' not present on object '%s'; skipping incremental filter to avoid API error. Sync will read all records.",
                    cursor_field_name,
                    self.object_name,
                )
        elif cursor_field_name:
            # Single-slice fallback: open-ended GTE filter for non-datetime cursors or first sync
            cursor_value = (stream_state or {}).get(cursor_field_name)
            if cursor_value:
                if cursor_field_name in schema_properties:
                    body["where"] = {
                        "conditions": [{"name": cursor_field_name, "alias": "A", "value": [cursor_value], "operator": "GTE"}],
                        "expression": "A",
                    }
                else:
                    logger.warning(
                        "Cursor field '%s' not present on object '%s'; skipping incremental filter to avoid API error. Sync will read all records.",
                        cursor_field_name,
                        self.object_name,
                    )

        return body

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        logger = logging.getLogger(__name__)
        try:
            body = response.json()
            if body.get("result") == False:
                logger.warning(f"Skipping records for object '{self.object_name}' due to error: {body.get('errorDesc', '')}")
                return
            records = body.get("data", {}).get("records", [])
        except Exception as e:
            logger.error(f"Failed to parse response for object '{self.object_name}': {e}")
            return

        schema = self.get_json_schema()
        properties = schema.get("properties", {})
        
        # Identify date and datetime fields from schema
        date_fields = [
            field_name 
            for field_name, field_schema in properties.items() 
            if field_schema.get("format") in ["date", "date-time"]
        ]
        
        # Transform empty strings to null for date/datetime fields, and advance self._state
        # per record so CDK 0.90's _observe_state() reads the correct cursor value for each
        # mid-stream STATE checkpoint (state_checkpoint_interval) and for the final checkpoint.
        cursor = self._effective_cursor_field()
        for record in records:
            for field_name in date_fields:
                if field_name in record and record[field_name] == "":
                    record[field_name] = None
            if cursor and record.get(cursor):
                current = self._state.get(cursor, "")
                new_val = record[cursor]
                if new_val > current:
                    self._state = {cursor: new_val}
            yield record

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        logger = logging.getLogger(__name__)
        try:
            body = response.json()
            if body.get("result") == False:
                logger.warning(
                    f"Error response for object '{self.object_name}' at offset {self.offset}, "
                    f"continuing to next page: {body.get('errorDesc', '')}"
                )
                self.offset = self.offset + self.limit
                return self.offset
            data = body.get("data", {}).get("records", [])
            if len(data) < self.limit:
                return None
            prev_offset = self.offset
            self.offset = self.offset + self.limit
            logger.debug(
                "Stream '%s': fetched page at offset %d, advancing to %d",
                self.object_name, prev_offset, self.offset,
            )
            return self.offset
        except Exception as e:
            logger.error(
                f"Failed to parse next page token for object '{self.object_name}' at offset {self.offset}, "
                f"continuing to next page: {e}"
            )
            self.offset = self.offset + self.limit
            return self.offset

    def get_json_schema(self) -> Mapping[str, Any]:
        if self.json_schema is not None:
            return self.json_schema

        base_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {}
        }

        url = f"{self.url_base}meta/services/objects/{self.name}/describe?idd=true"

        while True:
            try:
                session = requests.get(url, auth=self.authenticator)
                body = session.json()
                
                # Check if the response indicates success
                if body.get('result', True):
                    full_schema = base_schema
                    fields = body['data'][0]['fields']
                    full_schema = self.dynamic_schema(full_schema, fields)
                    self.json_schema = full_schema
                    break
                else:
                    # Handle rate limiting
                    if body.get('result') == False and body.get('errorCode') == 'GS_APIG_2404':
                        print("Rate limit reached. Will retry after 60 seconds...")
                        sleep(60)
                        continue
            except requests.exceptions.RequestException:
                self.json_schema = base_schema
                break

        # Workaround for missing gsid in many objects. Primary Key is either None or "Gsid".
        if self.json_schema['properties'].get('Gsid') is not None:
            self._primary_key = "Gsid"
        return self.json_schema

    def get_select_columns(self):
        json_schema = self.get_json_schema()
        return [key for key in json_schema['properties']]

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"data/objects/query/{self.name}"
