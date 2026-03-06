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


def test_get_updated_state_advances_cursor(patch_base_class, mock_authenticator, mocker):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"ModifiedDate": {}}})
    current_state = {"ModifiedDate": "2024-01-01T00:00:00Z"}
    latest_record = {"Gsid": "abc", "ModifiedDate": "2024-02-01T12:00:00Z"}
    new_state = stream.get_updated_state(current_state, latest_record)
    assert new_state == {"ModifiedDate": "2024-02-01T12:00:00Z"}


def test_get_updated_state_keeps_cursor_when_record_older(patch_base_class, mock_authenticator, mocker):
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"ModifiedDate": {}}})
    current_state = {"ModifiedDate": "2024-02-01T12:00:00Z"}
    latest_record = {"Gsid": "abc", "ModifiedDate": "2024-01-01T00:00:00Z"}
    new_state = stream.get_updated_state(current_state, latest_record)
    assert new_state == {"ModifiedDate": "2024-02-01T12:00:00Z"}


def test_get_updated_state_no_cursor_returns_current_state(patch_base_class, mock_authenticator, mocker):
    """When stream has no cursor field (e.g. ModifiedDate not in schema), state is unchanged."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"Gsid": {}, "Name": {}}})
    current_state = {"other_key": "value"}
    latest_record = {"Gsid": "abc", "Name": "foo"}
    new_state = stream.get_updated_state(current_state, latest_record)
    assert new_state == {"other_key": "value"}


def test_as_airbyte_stream_with_modified_date(patch_base_class, mock_authenticator, mocker):
    """Streams with ModifiedDate should advertise both sync modes, source-defined cursor, and ModifiedDate as default cursor field."""
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
    assert airbyte_stream.source_defined_cursor is True
    assert airbyte_stream.default_cursor_field == ["ModifiedDate"]


def test_as_airbyte_stream_without_modified_date(patch_base_class, mock_authenticator, mocker):
    """Streams without ModifiedDate should still advertise both sync modes, but with no source-defined cursor and no default cursor field,
    so the UI defaults to Full Refresh | Append while still allowing Incremental | Append with a user-chosen cursor."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(
        stream,
        "get_json_schema",
        return_value={"properties": {"Gsid": {}, "Date": {"type": ["null", "string"], "format": "date"}}},
    )
    airbyte_stream = stream.as_airbyte_stream()

    from airbyte_cdk.models import SyncMode as SM
    assert SM.full_refresh in airbyte_stream.supported_sync_modes
    assert SM.incremental in airbyte_stream.supported_sync_modes
    assert airbyte_stream.source_defined_cursor is False
    assert not airbyte_stream.default_cursor_field


def test_source_defined_cursor_true_when_modified_date_present(patch_base_class, mock_authenticator, mocker):
    """source_defined_cursor should be True when ModifiedDate is in the stream schema."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"Gsid": {}, "ModifiedDate": {}}})
    assert stream.source_defined_cursor is True


def test_source_defined_cursor_false_when_modified_date_absent(patch_base_class, mock_authenticator, mocker):
    """source_defined_cursor should be False when ModifiedDate is not in the stream schema."""
    stream = GainsightCsObjectStream(name=GAINSIGHT_STREAM_NAME, authenticator=mock_authenticator)
    mocker.patch.object(stream, "get_json_schema", return_value={"properties": {"Gsid": {}, "Name": {}}})
    assert stream.source_defined_cursor is False
