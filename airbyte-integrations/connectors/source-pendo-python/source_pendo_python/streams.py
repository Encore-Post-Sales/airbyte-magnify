from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream


class PendoPythonStream(HttpStream, ABC):
    url_base = "https://app.pendo.io/api/v1/"
    primary_key = "id"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from response.json()

    # Method to get an Airbyte field schema for a given Pendo field type
    def get_valid_field_info(self, field_type) -> dict:
        output_types = []
        if field_type == "time":
            output_types = ["null", "integer"]
        elif field_type == "float":
            output_types = ["null", "number"]
        elif field_type == "list":
            output_types = ["null", "array", "string"]
        elif field_type == "":
            output_types = ["null", "array", "string", "integer", "boolean"]
        else:
            output_types = ["null", field_type]
        return {"type": output_types}

    # Build the Airbyte stream schema from Pendo metadata
    def build_schema(self, full_schema, metadata):
        for key in metadata:
            if key != "auto" and key != "auto__323232":  # Skipping for now while we understand Pendo schema and what auto_323232 is
                fields = {}
                for field in metadata[key]:
                    field_type = metadata[key][field]["Type"]
                    fields[field] = self.get_valid_field_info(field_type)

                full_schema["properties"]["metadata"]["properties"][key] = {"type": ["null", "object"], "properties": fields}
        return full_schema


# Airbyte Streams using the Pendo /aggregation endpoint (Currently only Account and Visitor)
class PendoAggregationStream(PendoPythonStream):
    json_schema = None  # Field to store dynamically built Airbyte Stream Schema
    page_size = 100

    @property
    def http_method(self) -> str:
        return "POST"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "aggregation"

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"Content-Type": "application/json"}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json().get("results", [])
        if len(data) < self.page_size:
            return None
        return data[-1][self.primary_key]

    def parse_response(
        self, response: requests.Response, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, **kwargss
    ) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        yield from response.json().get("results", [])

    # Build /aggregation endpoint payload with pagination for a given source and requestId
    def build_request_body(self, requestId, source, next_page_token) -> Optional[Mapping[str, Any]]:
        request_body = {
            "response": {"mimeType": "application/json"},
            "request": {
                "requestId": requestId,
                "pipeline": [
                    {"source": source},
                    {"sort": [self.primary_key]},
                    {"limit": self.page_size},
                ],
            },
        }

        if next_page_token is not None:
            request_body["request"]["pipeline"].insert(2, {"filter": f'{self.primary_key} > "{next_page_token}"'})

        return request_body


# Airbyte Streams using the Pendo /aggregation endpoint (Currently only Account and Visitor)
class PendoTimeSeriesAggregationStream(PendoPythonStream):
    json_schema = None  # Field to store dynamically built Airbyte Stream Schema
    MAX_DAYS = -730  # Two Years TODO : Start the window relative to the ingest start date.
    DAY_PAGE_SIZE = 21
    current_day = MAX_DAYS

    @property
    def http_method(self) -> str:
        return "POST"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "aggregation"

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"Content-Type": "application/json"}

    def next_page_token(self, response: requests.Response) -> Optional[int]:
        self.current_day = self.current_day + self.DAY_PAGE_SIZE

        if self.current_day >= 0:
            return None

        return self.current_day

    def parse_response(
        self, response: requests.Response, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, **kwargss
    ) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        yield from response.json().get("results", [])

    # Build /aggregation endpoint payload with pagination for a given source and requestId
    def build_request_body(self, requestId, source, next_page_token) -> Optional[Mapping[str, Any]]:
        request_body = {
            "response": {"mimeType": "application/json"},
            "request": {
                "requestId": requestId,
                "pipeline": [
                    {"source": source},
                ],
            },
        }

        if next_page_token is None:
            request_body["request"]["pipeline"][0]["source"]["timeSeries"]["first"] = f"now() {self.MAX_DAYS} * 24*60*60*1000"
        else:
            request_body["request"]["pipeline"][0]["source"]["timeSeries"]["first"] = f"now() {next_page_token} * 24*60*60*1000"

        return request_body


# class PendoEventsStream(PendoAggregationStream):
#     TODO: Create A Class for the Event Streams feature, page, guide


class Feature(PendoPythonStream):
    name = "feature"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return "feature"


class Guide(PendoPythonStream):
    name = "guide"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return "guide"


class Page(PendoPythonStream):
    name = "page"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return "page"


class Report(PendoPythonStream):
    name = "report"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return "report"


class ReportResult(PendoPythonStream):
    json_schema = None  # Field to store dynamically built Airbyte Stream Schema
    primary_key = "reportId"

    def __init__(self, report: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.report = report
        self.report_name = f"report_result_{report['id']}"

    @property
    def name(self):
        return self.report_name

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"report/{self.report['id']}/results.json"

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        for record in response.json():
            yield self.transform(record=record)

    def transform(self, record: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        record["reportId"] = self.report['id']
        return record

    # Method to infer schema types from JSON response
    def infer_type(self, value: Any):
        if isinstance(value, str):
            return {"type": ["null", "string"]}
        if isinstance(value, bool):
            return {"type": ["null", "boolean"]}
        if isinstance(value, float):
            return {"type": ["null", "number"]}
        if isinstance(value, int):
            return {"type": ["null", "integer"]}
        if isinstance(value, list):
            return {"type": ["null", "array"]}
        if isinstance(value, dict):
            return {"type": ["null", "object"]}
        return {"type": ["null", "string"]}

    def get_json_schema(self) -> Mapping[str, Any]:
        if self.json_schema is None:
            schema = {
                "type": "object",
                "$schema": "http://json-schema.org/schema#",
                "properties": {
                    "reportId": {
                        "type": "string"
                    }
                }
            }

            url = f"{PendoPythonStream.url_base}{self.path()}"
            auth_headers = self.authenticator.get_auth_header()
            try:
                session = requests.get(url, headers=auth_headers)
                body = session.json()
                if body is not None and len(body) != 0:
                    for result in body:
                        for field in result:
                            if result[field] is not None:
                                schema["properties"][field] = self.infer_type(result[field])
                self.json_schema = schema
            except requests.exceptions.RequestException:
                print("Error fetching sample Pendo Report Results")
                self.json_schema = schema

        return self.json_schema


class VisitorMetadata(PendoPythonStream):
    name = "visitor_metadata"
    primary_key = []

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return "metadata/schema/visitor"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from [response.json()]


class AccountMetadata(PendoPythonStream):
    name = "account_metadata"
    primary_key = []

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return "metadata/schema/account"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield from [response.json()]


class Visitor(PendoAggregationStream):
    primary_key = "visitorId"

    name = "visitor"

    def get_json_schema(self) -> Mapping[str, Any]:
        if self.json_schema is not None:
            return self.json_schema

        base_schema = super().get_json_schema()
        url = f"{PendoPythonStream.url_base}metadata/schema/visitor"
        auth_headers = self.authenticator.get_auth_header()
        try:
            session = requests.get(url, headers=auth_headers)
            body = session.json()

            full_schema = base_schema

            # Not all fields are getting returned by Pendo's metadata apis so we need to do some manual construction
            full_schema["properties"]["metadata"]["properties"]["auto__323232"] = {"type": ["null", "object"]}

            auto_fields = {
                "lastupdated": {"type": ["null", "integer"]},
                "idhash": {"type": ["null", "integer"]},
                "lastuseragent": {"type": ["null", "string"]},
                "lastmetadataupdate_agent": {"type": ["null", "integer"]},
            }
            for key in body["auto"]:
                auto_fields[key] = self.get_valid_field_info(body["auto"][key]["Type"])
            full_schema["properties"]["metadata"]["properties"]["auto"]["properties"] = auto_fields
            full_schema["properties"]["metadata"]["properties"]["auto__323232"]["properties"] = auto_fields

            full_schema = self.build_schema(full_schema, body)
            self.json_schema = full_schema
        except requests.exceptions.RequestException:
            self.json_schema = base_schema
        return self.json_schema

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping[str, Any]]:
        source = {"visitors": {"identified": True}}
        return self.build_request_body("visitor-list", source, next_page_token)


class Account(PendoAggregationStream):
    primary_key = "accountId"

    name = "account"

    def get_json_schema(self) -> Mapping[str, Any]:
        if self.json_schema is not None:
            return self.json_schema

        base_schema = super().get_json_schema()
        url = f"{PendoPythonStream.url_base}metadata/schema/account"
        auth_headers = self.authenticator.get_auth_header()
        try:
            session = requests.get(url, headers=auth_headers)
            body = session.json()

            full_schema = base_schema

            # Not all fields are getting returned by Pendo's metadata apis so we need to do some manual construction
            full_schema["properties"]["metadata"]["properties"]["auto__323232"] = {"type": ["null", "object"]}

            auto_fields = {"lastupdated": {"type": ["null", "integer"]}, "idhash": {"type": ["null", "integer"]}}
            for key in body["auto"]:
                auto_fields[key] = self.get_valid_field_info(body["auto"][key]["Type"])
            full_schema["properties"]["metadata"]["properties"]["auto"]["properties"] = auto_fields
            full_schema["properties"]["metadata"]["properties"]["auto__323232"]["properties"] = auto_fields

            full_schema = self.build_schema(full_schema, body)
            self.json_schema = full_schema
        except requests.exceptions.RequestException:
            self.json_schema = base_schema
        return self.json_schema

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping[str, Any]]:
        source = {"accounts": {}}
        return self.build_request_body("account-list", source, next_page_token)


class PageEvents(PendoTimeSeriesAggregationStream):
    name = "page_events"
    primary_key = ["pageId", "day", "visitorId", "accountId", "server", "remoteIp", "userAgent"]

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: int = None,
    ) -> Optional[Mapping[str, Any]]:
        source = {
            "pageEvents": None,
            "timeSeries": {"first": "", "count": self.DAY_PAGE_SIZE, "period": "dayRange"},
        }
        return self.build_request_body("page-events", source, next_page_token)


class FeatureEvents(PendoTimeSeriesAggregationStream):
    name = "feature_events"

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: int = None,
    ) -> Optional[Mapping[str, Any]]:
        source = {"featureEvents": None, "timeSeries": {"first": "", "count": self.DAY_PAGE_SIZE, "period": "dayRange"}}

        return self.build_request_body("feature_events", source, next_page_token)


class GuideEvents(PendoTimeSeriesAggregationStream):
    name = "guide_events"

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: int = None,
    ) -> Optional[Mapping[str, Any]]:
        source = {"guideEvents": None, "timeSeries": {"first": "", "count": self.DAY_PAGE_SIZE, "period": "dayRange"}}

        return self.build_request_body("guide_events", source, next_page_token)
