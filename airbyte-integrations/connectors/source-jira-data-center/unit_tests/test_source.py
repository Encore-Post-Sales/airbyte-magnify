#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

import pytest
import responses
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from source_jira_data_center.source import SourceJiraDataCenter


@responses.activate
def test_streams(config):
    source = SourceJiraDataCenter()
    streams = source.streams(config)
    expected_streams_number = 55
    assert len(streams) == expected_streams_number


@responses.activate
def test_check_connection_config_no_access_to_one_stream(config, caplog, projects_response, avatars_response):
    # Data Center project API returns a raw array
    project_list = projects_response.get("values", projects_response) if isinstance(projects_response, dict) else projects_response
    responses.add(
        responses.GET,
        f"https://{config['domain']}/rest/api/2/project?expand=description%2Clead&includeArchived=true",
        json=project_list,
    )
    responses.add(
        responses.GET,
        f"https://{config['domain']}/rest/api/2/applicationrole",
        status=401,
    )
    responses.add(
        responses.GET,
        f"https://{config['domain']}/rest/api/2/avatar/issuetype/system",
        json=avatars_response,
    )
    responses.add(responses.GET, f"https://{config['domain']}/rest/api/2/label?maxResults=50", status=401)
    source = SourceJiraDataCenter()
    logger_mock = MagicMock()
    assert source.check_connection(logger=logger_mock, config=config) == (True, None)


@responses.activate
def test_check_connection_404_error(config):
    responses.add(
        responses.GET,
        f"https://{config['domain']}/rest/api/2/project?expand=description%2Clead&includeArchived=true",
        status=404,
    )
    responses.add(responses.GET, f"https://{config['domain']}/rest/api/2/label?maxResults=50", status=404)
    source = SourceJiraDataCenter()
    logger_mock = MagicMock()
    with pytest.raises(AirbyteTracedException) as e:
        source.check_connection(logger=logger_mock, config=config)

    assert (
        e.value.message == "Config validation error: please check that your domain is valid and does not include protocol (e.g: jira.example.com)."
    )


def test_get_authenticator(config):
    source = SourceJiraDataCenter()
    authenticator = source.get_authenticator(config=config)

    assert authenticator.get_auth_header() == {"Authorization": "Bearer token"}
