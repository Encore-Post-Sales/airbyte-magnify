#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
from http import HTTPStatus

import responses
from conftest import find_stream
from source_jira_data_center.streams import Issues, Projects
from source_jira_data_center.utils import read_full_refresh


@responses.activate
def test_pagination_projects():
    """Data Center GET /rest/api/2/project returns a list (no /search, no pagination)."""
    domain = "domain.com"
    responses.add(
        responses.GET,
        f"https://{domain}/rest/api/2/project",
        json=[{"id": "1"}, {"id": "2"}, {"id": "3"}, {"id": "4"}, {"id": "5"}, {"id": "6"}],
        content_type="application/json",
    )

    stream = Projects(authenticator=None, domain=domain, projects=[])
    records = list(read_full_refresh(stream))
    assert records == [{"id": "1"}, {"id": "2"}, {"id": "3"}, {"id": "4"}, {"id": "5"}, {"id": "6"}]


@responses.activate
def test_pagination_issues():
    domain = "domain.com"
    responses_json = [
        (
            HTTPStatus.OK,
            {},
            json.dumps(
                {
                    "startAt": 0,
                    "maxResults": 2,
                    "total": 6,
                    "issues": [{"id": "1", "updated": "2022-01-01"}, {"id": "2", "updated": "2022-01-01"}],
                }
            ),
        ),
        (
            HTTPStatus.OK,
            {},
            json.dumps(
                {
                    "startAt": 2,
                    "maxResults": 2,
                    "total": 6,
                    "issues": [{"id": "3", "updated": "2022-01-01"}, {"id": "4", "updated": "2022-01-01"}],
                }
            ),
        ),
        (
            HTTPStatus.OK,
            {},
            json.dumps(
                {
                    "startAt": 4,
                    "maxResults": 2,
                    "total": 6,
                    "issues": [{"id": "5", "updated": "2022-01-01"}, {"id": "6", "updated": "2022-01-01"}],
                }
            ),
        ),
    ]

    responses.add_callback(
        responses.GET,
        f"https://{domain}/rest/api/2/search",
        callback=lambda request: responses_json.pop(0),
        content_type="application/json",
    )

    stream = Issues(authenticator=None, domain=domain, projects=[])
    stream.transform = lambda record, **kwargs: record
    records = list(read_full_refresh(stream))
    assert records == [
        {"id": "1", "updated": "2022-01-01"},
        {"id": "2", "updated": "2022-01-01"},
        {"id": "3", "updated": "2022-01-01"},
        {"id": "4", "updated": "2022-01-01"},
        {"id": "5", "updated": "2022-01-01"},
        {"id": "6", "updated": "2022-01-01"},
    ]


@responses.activate
def test_pagination_users(config):
    domain = "domain.com"
    config["domain"] = domain
    responses_json = [
        (HTTPStatus.OK, {}, json.dumps([{"self": "user1"}, {"self": "user2"}])),
        (HTTPStatus.OK, {}, json.dumps([{"self": "user3"}, {"self": "user4"}])),
        (HTTPStatus.OK, {}, json.dumps([{"self": "user5"}])),
    ]

    responses.add_callback(
        responses.GET,
        f"https://{domain}/rest/api/2/users/search",
        callback=lambda request: responses_json.pop(0),
        content_type="application/json",
    )

    stream = find_stream("users", config)
    stream.retriever.paginator.pagination_strategy.page_size = 2
    records = list(read_full_refresh(stream))
    expected_records = [
        {"self": "user1"},
        {"self": "user2"},
        {"self": "user3"},
        {"self": "user4"},
        {"self": "user5"},
    ]

    for rec, exp in zip(records, expected_records):
        assert dict(rec) == exp, f"Failed at {rec} vs {exp}"
