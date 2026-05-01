#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest
import responses
from unittest.mock import MagicMock, patch

from source_gainsight_cs.source import SourceGainsightCs, GainsightCsAuthenticator


GAINSIGHT_DOMAIN_URL = "https://fake-domain.gainsightcloud.com"
FAKE_CLIENT_ID = "my-client-id"
FAKE_CLIENT_SECRET = "my-client-secret"
FAKE_ACCESS_TOKEN = "fake-access-token"
GAINSIGHT_OBJECTS = ["person", "playbook", "gsuer", "company", "custom1", "custom2"]


@pytest.fixture(name="config")
def config_fixture():
    return {
        "client_id": FAKE_CLIENT_ID,
        "client_secret": FAKE_CLIENT_SECRET,
        "domain_url": GAINSIGHT_DOMAIN_URL,
    }


@pytest.fixture
def mock_get_objects(mocker):
    mocker.patch.object(SourceGainsightCs, "get_objects", return_value=GAINSIGHT_OBJECTS)


def test_gainsight_cs_authenticator(config):
    authenticator = GainsightCsAuthenticator(config)
    assert authenticator._client_id == FAKE_CLIENT_ID
    assert authenticator._client_secret == FAKE_CLIENT_SECRET
    assert authenticator.domain_url == GAINSIGHT_DOMAIN_URL


@responses.activate
def test_check_connection(config):
    responses.add(
        responses.POST,
        f"{GAINSIGHT_DOMAIN_URL}/v1/users/m2m/oauth/token",
        json={"access_token": FAKE_ACCESS_TOKEN, "expires_in": 3600},
    )
    responses.add(
        responses.GET,
        f"{GAINSIGHT_DOMAIN_URL}/v1/meta/services/objects/Person/describe?idd=true",
        json=[],
    )
    source = SourceGainsightCs()
    logger_mock = MagicMock()
    ok, error_msg = source.check_connection(logger_mock, config)

    assert ok
    assert not error_msg


def test_check_connection_fail():
    source = SourceGainsightCs()
    logger_mock = MagicMock()
    ok, error_msg = source.check_connection(logger_mock, {})

    assert not ok
    assert error_msg


def test_streams(config, mock_get_objects):
    source = SourceGainsightCs()
    streams = source.streams(config)
    expected_streams_number = len(GAINSIGHT_OBJECTS)
    assert len(streams) == expected_streams_number
