#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from source_gainsight_cs.streams import GainsightCsStream, GainsightCsObjectStream

GAINSIGHT_DOMAIN_URL = "https://fake-domain.gainsightcloud.com"
GAINSIGHT_STREAM_NAME = "fake_name"


@pytest.fixture
def patch_base_class(mocker):
    mocker.patch.object(GainsightCsStream, "url_base", f"{GAINSIGHT_DOMAIN_URL}/v1/")
    mocker.patch.object(GainsightCsStream, "__abstractmethods__", set())
    mocker.patch.object(GainsightCsObjectStream, "limit", 5)


@pytest.fixture
def mock_authenticator():
    auth = MagicMock()
    auth.domain_url = GAINSIGHT_DOMAIN_URL
    return auth


def test_request_params(patch_base_class, mock_authenticator):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = {}
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class, mock_authenticator):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    response = MagicMock()
    records = [{"test_id": "id1"}, {"test_id": "id2"}, {"test_id": "id3"}, {"test_id": "id4"}, {"test_id": "id5"}]
    response.json.return_value = {
        "data": {
            "records": records
        }
    }
    inputs = {"response": response}
    expected_token = stream.offset + stream.limit
    assert stream.next_page_token(**inputs) == expected_token


def test_next_page_token_end(patch_base_class, mock_authenticator):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    response = MagicMock()
    records = [{"test_id": "id1"}, {"test_id": "id2"}]
    response.json.return_value = {
        "data": {
            "records": records
        }
    }
    inputs = {"response": response}
    expected_token = None
    assert stream.next_page_token(**inputs) == expected_token


def test_next_page_token_error_response_returns_none(patch_base_class, mock_authenticator):
    """An API error response (result=False) skips past the errored page by advancing the offset."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    response = MagicMock()
    response.json.return_value = {"result": False, "errorDesc": "Some API error"}
    expected_token = stream.offset + stream.limit
    assert stream.next_page_token(response=response) == expected_token


def test_next_page_token_exception_returns_none(patch_base_class, mock_authenticator):
    """A response that cannot be parsed advances the offset to skip past the problematic page."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    response = MagicMock()
    response.json.side_effect = Exception("malformed JSON")
    expected_token = stream.offset + stream.limit
    assert stream.next_page_token(response=response) == expected_token


def test_parse_response(patch_base_class, mock_authenticator, mocker):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"test_id": {}}})
    response = MagicMock()
    records = [{"test_id": "id1"}, {"test_id": "id2"}]
    expected_parsed_object = {
        "data": {
            "records": records
        }
    }
    response.json.return_value = expected_parsed_object
    inputs = {"response": response}
    assert next(stream.parse_response(**inputs)) == records[0]


def test_request_headers(patch_base_class, mock_authenticator):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_headers = {}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class, mock_authenticator):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    expected_method = "POST"
    assert stream.http_method == expected_method


def test_path(patch_base_class, mock_authenticator):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    assert stream.path(**inputs) == f"data/objects/query/{GAINSIGHT_STREAM_NAME}"


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, mock_authenticator, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class, mock_authenticator):
    response_mock = MagicMock()
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time


def test_request_body_json_no_state_includes_no_where(patch_base_class, mock_authenticator, mocker):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_select_columns", return_value=["Gsid", "ModifiedDate"])
    body = stream.request_body_json(stream_state=None, stream_slice=None, next_page_token=None)
    assert "select" in body
    assert body["select"] == ["Gsid", "ModifiedDate"]
    assert "limit" in body
    assert "offset" in body
    assert "where" not in body


def test_request_body_json_with_state_includes_where_gte(patch_base_class, mock_authenticator, mocker):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_select_columns", return_value=["Gsid", "ModifiedDate"])
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"Gsid": {}, "ModifiedDate": {}}})
    stream_state = {"ModifiedDate": "2024-01-15T10:00:00Z"}
    body = stream.request_body_json(stream_state=stream_state, stream_slice=None, next_page_token=None)
    assert body.get("where") == {
        "conditions": [
            {"name": "ModifiedDate", "alias": "A", "value": ["2024-01-15T10:00:00Z"], "operator": "GTE"}
        ],
        "expression": "A",
    }


def test_request_body_json_state_but_cursor_not_in_schema_no_where(patch_base_class, mock_authenticator, mocker):
    """When cursor field is not in stream schema, we must not add where to avoid API error."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_select_columns", return_value=["Gsid", "Name"])
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"Gsid": {}, "Name": {}}})
    stream_state = {"ModifiedDate": "2024-01-15T10:00:00Z"}
    body = stream.request_body_json(stream_state=stream_state, stream_slice=None, next_page_token=None)
    assert "where" not in body


def test_as_airbyte_stream(patch_base_class, mock_authenticator, mocker):
    """All streams advertise both sync modes with no source-defined cursor.
    The catalog always supplies the cursor field for incremental syncs."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(
        stream,
        "get_json_schema",
        return_value={"properties": {"Gsid": {}, "ModifiedDate": {"type": ["null", "string"], "format": "date-time"}}},
    )
    airbyte_stream = stream.as_airbyte_stream()

    from airbyte_cdk.models import SyncMode as SM
    assert SM.full_refresh in airbyte_stream.supported_sync_modes
    assert SM.incremental in airbyte_stream.supported_sync_modes
    assert airbyte_stream.source_defined_cursor is False
    assert not airbyte_stream.default_cursor_field


def test_state_checkpoint_interval_is_set(patch_base_class, mock_authenticator):
    """state_checkpoint_interval must be set so the CDK emits STATE messages mid-stream
    for large streams like activity_timeline that would otherwise never checkpoint."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    assert stream.state_checkpoint_interval is not None
    assert stream.state_checkpoint_interval > 0


def test_read_records_sets_and_clears_current_slice(patch_base_class, mock_authenticator, mocker):
    """_current_slice must be set during read_records and cleared afterwards,
    so the finally block has access to the slice boundaries to advance self._state."""
    from airbyte_cdk.models import SyncMode

    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"ModifiedDate": {}}})

    captured_slice_during_read = {}
    original_super_read = lambda *a, **kw: iter([{"ModifiedDate": "2026-03-05T00:00:00Z"}])

    def fake_super_read(*args, **kwargs):
        captured_slice_during_read["slice"] = stream._current_slice
        return iter([{"ModifiedDate": "2026-03-05T00:00:00Z"}])

    mocker.patch("airbyte_cdk.sources.streams.http.HttpStream.read_records", side_effect=fake_super_read)

    test_slice = {"cursor_field": "ModifiedDate", "start": "2026-03-01T00:00:00Z", "end": "2026-03-10T21:08:06Z"}
    list(stream.read_records(SyncMode.incremental, stream_slice=test_slice))

    assert captured_slice_during_read["slice"] == test_slice
    assert stream._current_slice is None


def test_stream_slices_uses_catalog_cursor_when_no_modified_date(patch_base_class, mock_authenticator, mocker):
    """When stream has no ModifiedDate but catalog selects incremental with a different cursor (e.g. LastModifiedDate),
    stream_slices must use the passed cursor_field for time-window slicing and state key."""
    from airbyte_cdk.models import SyncMode

    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    # Schema has LastModifiedDate (date-time) but NOT ModifiedDate
    mocker.patch.object(
        stream,
        "get_json_schema",
        return_value={
            "properties": {
                "Gsid": {},
                "LastModifiedDate": {"type": ["null", "string"], "format": "date-time"},
            }
        },
    )

    # No cursor_field passed -> stream has no default cursor -> single full-scan slice
    slices_no_cursor = list(
        stream.stream_slices(
            sync_mode=SyncMode.incremental,
            cursor_field=None,
            stream_state={"LastModifiedDate": "2024-01-01T00:00:00Z"},
        )
    )
    assert len(slices_no_cursor) == 1
    assert slices_no_cursor[0] == {}

    # cursor_field passed from catalog (e.g. user chose LastModifiedDate) -> use it for slicing
    slices_with_cursor = list(
        stream.stream_slices(
            sync_mode=SyncMode.incremental,
            cursor_field=["LastModifiedDate"],
            stream_state={"LastModifiedDate": "2024-01-01T00:00:00Z"},
        )
    )
    # Should yield time-window slices with cursor_field LastModifiedDate
    assert len(slices_with_cursor) >= 1
    first = slices_with_cursor[0]
    assert first.get("cursor_field") == "LastModifiedDate"
    assert "start" in first and "end" in first
    assert first.get("cursor_format") == "date-time"


def test_parse_response_advances_state_per_record(patch_base_class, mock_authenticator, mocker):
    """parse_response must update self._state for each record so that CDK 0.90's
    _observe_state() reads the correct cursor value for mid-stream and final STATE checkpoints."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(
        stream,
        "get_json_schema",
        return_value={"properties": {"Gsid": {}, "ModifiedDate": {"type": ["null", "string"], "format": "date-time"}}},
    )
    stream._cursor_field_override = "ModifiedDate"

    response = MagicMock()
    response.json.return_value = {
        "result": True,
        "data": {
            "records": [
                {"Gsid": "a", "ModifiedDate": "2024-01-01T00:00:00Z"},
                {"Gsid": "b", "ModifiedDate": "2024-03-01T00:00:00Z"},
                {"Gsid": "c", "ModifiedDate": "2024-02-01T00:00:00Z"},
            ]
        },
    }

    records = list(stream.parse_response(response))

    assert len(records) == 3
    # State must reflect the highest cursor value seen, not just the last record
    assert stream.state == {"ModifiedDate": "2024-03-01T00:00:00Z"}
