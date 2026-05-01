# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from unittest.mock import MagicMock, patch

import pytest
import requests
from components import IntercomErrorHandler, IntercomScrollRetriever, ResetCursorSignal

def test_slicer():
    date_time_dict = {"updated_at": 1662459010}
    slicer = IncrementalSingleSliceCursor(config={}, parameters={}, cursor_field="updated_at")
    slicer.observe(date_time_dict, date_time_dict)
    slicer.close_slice(date_time_dict)
    assert slicer.get_stream_state() == date_time_dict
    assert slicer.get_request_headers() == {}
    assert slicer.get_request_body_data() == {}
    assert slicer.get_request_body_json() == {}


@pytest.mark.parametrize(
    "last_record, expected, records",
    [
        (
            {"first_stream_cursor": 1662459010},
            {
                "first_stream_cursor": 1662459010,
                "prior_state": {"first_stream_cursor": 1662459010, "parent_stream_name": {"parent_cursor_field": 1662459010}},
                "parent_stream_name": {"parent_cursor_field": 1662459010},
            },
            [{"parent_cursor_field": 1662459010, "first_stream_cursor": 1662459010}],
        )
    ],
)
def test_sub_slicer(last_record, expected, records):
    parent_stream = Mock(spec=Stream)
    parent_stream.name = "parent_stream_name"
    parent_stream.cursor_field = "parent_cursor_field"
    parent_stream.state = {"parent_stream_name": {"parent_cursor_field": 1662459010}}
    parent_stream.stream_slices.return_value = [{"a slice": "value"}]
    parent_stream.read_records = MagicMock(return_value=records)

    parent_config = ParentStreamConfig(
        stream=parent_stream,
        parent_key="first_stream_cursor",
        partition_field="first_stream_id",
        parameters={},
        config={},
    )

    slicer = IncrementalSubstreamSlicerCursor(
        config={}, parameters={}, cursor_field="first_stream_cursor", parent_stream_configs=[parent_config], parent_complete_fetch=True
    )
    slicer.set_initial_state(expected)
    stream_slice = next(slicer.stream_slices()) if records else {}
    slicer.observe(stream_slice, last_record)
    slicer.close_slice(stream_slice)
    assert slicer.get_stream_state() == expected

@pytest.mark.parametrize(
    "rate_limit_header, backoff_time",
    [
        ({"X-RateLimit-Limit": 167, "X-RateLimit-Remaining": 167}, 0.01),
        ({"X-RateLimit-Limit": 167, "X-RateLimit-Remaining": 100}, 0.01),
        ({"X-RateLimit-Limit": 167, "X-RateLimit-Remaining": 83}, 1.5),
        ({"X-RateLimit-Limit": 167, "X-RateLimit-Remaining": 16}, 8.0),
        ({}, 1.0),
    ],
)
def test_rate_limiter(components_module, rate_limit_header, backoff_time):
    IntercomRateLimiter = components_module.IntercomRateLimiter

    def check_backoff_time(t):
        """A replacer for original `IntercomRateLimiter.backoff_time`"""
        assert backoff_time == t, f"Expected {backoff_time}, got {t}"

    class Requester:
        @IntercomRateLimiter.balance_rate_limit()
        def interpret_response_status(self, response: requests.Response):
            """A stub for the decorator function being tested"""

    with patch.object(IntercomRateLimiter, "backoff_time") as backoff_time_mock:
        # Call `check_backoff_time` instead of original `IntercomRateLimiter.backoff_time` method
        backoff_time_mock.side_effect = check_backoff_time

        requester = Requester()

        # Prepare requester object with headers
        response = requests.models.Response()
        response.headers = rate_limit_header

        # Call a decorated method
        requester.interpret_response_status(response)


@pytest.fixture(autouse=True)
def reset_signal(request):
    """Reset ResetCursorSignal before and after each test to isolate state"""
    signal = ResetCursorSignal()
    signal.clear_reset()

    def clear_signal():
        signal.clear_reset()

    request.addfinalizer(clear_signal)


def test_reset_cursor_signal():
    # Get the singleton instance
    signal = ResetCursorSignal()

    # Test initial state
    assert signal.is_reset_triggered() is False

    # Test triggering reset
    signal.trigger_reset()
    assert signal.is_reset_triggered() is True

    # Test clearing reset
    signal.clear_reset()
    assert signal.is_reset_triggered() is False

    # Test singleton behavior
    signal2 = ResetCursorSignal()
    signal.trigger_reset()
    assert signal2.is_reset_triggered() is True


def test_intercom_error_handler():
    handler = IntercomErrorHandler(config={}, parameters={})

    # Test HTTP 500 error triggers reset and retries
    response_500 = requests.Response()
    response_500.status_code = 500
    resolution = handler.interpret_response(response_500)
    assert resolution.response_action == ResponseAction.RETRY
    assert resolution.failure_type == FailureType.transient_error
    assert "HTTP 500" in resolution.error_message
    assert ResetCursorSignal().is_reset_triggered() is True  # Reset should be triggered

    # Clear the reset signal for the next test case
    ResetCursorSignal().clear_reset()

    # Test non-500 error does not trigger reset and uses default behavior
    response_404 = requests.Response()
    response_404.status_code = 404
    resolution = handler.interpret_response(response_404)
    assert resolution.response_action == ResponseAction.FAIL  # Default behavior for 404
    assert ResetCursorSignal().is_reset_triggered() is False  # Reset should not be triggered


def test_intercom_scroll_retriever_initialization():
    # Mock dependencies
    requester = MagicMock()
    paginator = MagicMock()
    record_selector = MagicMock()
    config = {}
    parameters = {}

    retriever = IntercomScrollRetriever(
        name="test_stream", requester=requester, paginator=paginator, record_selector=record_selector, config=config, parameters=parameters
    )

    # Test stream_slicer is correctly initialized
    assert isinstance(retriever.stream_slicer, SinglePartitionRouter)


def test_intercom_scroll_retriever_next_page_token():
    # Mock dependencies
    requester = MagicMock()
    paginator = MagicMock()
    record_selector = MagicMock()
    config = {}
    parameters = {}

    # Create a fresh retriever instance for this test
    retriever = IntercomScrollRetriever(
        name="test_stream", requester=requester, paginator=paginator, record_selector=record_selector, config=config, parameters=parameters
    )

    # Mock response and paginator behavior
    response = MagicMock()
    paginator.next_page_token.return_value = {"next_page_token": "next_cursor"}

    # Test when reset is not triggered
    token = retriever._next_page_token(response, 10, None, None)
    assert token == {"next_page_token": "next_cursor"}

    # Reset the retriever state by creating a new instance for the reset test
    retriever = IntercomScrollRetriever(
        name="test_stream", requester=requester, paginator=paginator, record_selector=record_selector, config=config, parameters=parameters
    )
    ResetCursorSignal().trigger_reset()
    token = retriever._next_page_token(response, 10, None, None)
    assert token == IntercomScrollRetriever.RESET_TOKEN
    assert ResetCursorSignal().is_reset_triggered() is False  # Reset should be cleared after use
