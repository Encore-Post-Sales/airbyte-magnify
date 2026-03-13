# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

"""
Validate that response fixtures conform to stream JSON schemas (Data Center v2 alignment).
Skips streams without a schema or fixture; validates first record from each fixture.
"""

import json
import os
import pytest

# Stream name -> (fixture filename, key path to list of records; None = root is the list or single record)
STREAM_FIXTURE_MAP = {
    "issues": ("issues.json", ["issues"]),
    "issue_comments": ("issue_comments.json", ["comments"]),
    "users": ("users.json", None),  # root is array
    "board_issues": ("board_issues.json", ["issues"]),
    "sprint_issues": ("sprint_issues.json", ["issues"]),
    "issue_worklogs": ("issue_worklogs.json", ["worklogs"]),
    "issue_watchers": ("issue_watchers.json", None),  # root is the record
    "issue_votes": ("issue_votes.json", None),
    "projects": ("projects.json", ["values"]),  # DC can return list; fixture uses values
    "project_avatars": ("projects_avatars.json", ["custom"]),
    "project_components": ("project_components.json", ["values"]),
    "project_email": ("project_email.json", None),
    "project_permission_schemes": ("project_permissions.json", ["levels"]),
    "project_versions": ("projects_versions.json", ["values"]),
    "issue_remote_links": ("issue_remote_links.json", None),
    "filters": ("filter.json", ["values"]),
    "filter_sharing": ("filter_sharing.json", None),
    "workflows": ("workflows.json", ["values"]),
    "workflow_statuses": ("workflow_statuses.json", None),
    "workflow_status_categories": ("workflow_status_categories.json", None),
    "issue_resolutions": ("issue_resolutions.json", None),
    "labels": ("labels.json", ["values"]),
    "permissions": ("permissions.json", None),
    "issue_fields": ("issue_fields.json", None),
    "boards": ("board.json", ["values"]),
    "sprints": ("sprints.json", ["values"]),
    "screens": ("screens.json", ["values"]),
    "groups": ("groups.json", ["values"]),
    "dashboards": ("dashboard.json", ["dashboards"]),
    "issue_notification_schemes": ("issue_notification_schemas.json", ["values"]),
    "issue_security_schemes": ("issue_security_schemes.json", ["issueSecuritySchemes"]),
    "issue_types": ("issue_type.json", None),
    "issue_link_types": ("issues_link_types.json", ["issueLinkTypes"]),
    "issue_navigator_settings": ("issues_navigator_settings.json", None),
    "jira_settings": ("jira_settings.json", None),
    "time_tracking": ("time_tracking.json", None),
    "issue_custom_field_contexts": ("issue_custom_field_contexts.json", ["values"]),
    "issue_custom_field_options": ("issue_custom_field_options.json", ["values"]),
    "application_roles": ("application_role.json", None),
    "avatars": ("avatars.json", None),
    "users_groups_detailed": ("users_groups_detailed.json", None),
    "issue_field_configurations": ("issues_field_configurations.json", ["values"]),
    "project_categories": ("projects_categories.json", None),
    "screen_tabs": ("screen_tabs.json", None),
    "screen_tab_fields": ("screen_tab_fields.json", None),
}


def _schema_dir():
    return os.path.join(os.path.dirname(__file__), "..", "source_jira_data_center", "schemas")


def _responses_dir():
    return os.path.join(os.path.dirname(__file__), "responses")


def _get_records(fixture_path: str, key_path) -> list:
    with open(fixture_path) as f:
        data = json.load(f)
    if key_path is None:
        if isinstance(data, list):
            return data
        return [data]
    for key in key_path:
        data = data[key]
    return data if isinstance(data, list) else [data]


@pytest.mark.parametrize("stream_name", sorted(STREAM_FIXTURE_MAP.keys()))
def test_fixture_matches_stream_schema(stream_name):
    """Validate first record from each fixture against the stream's JSON schema (v2 aligned)."""
    try:
        import jsonschema
    except ImportError:
        pytest.skip("jsonschema not installed")
    schema_path = os.path.join(_schema_dir(), f"{stream_name}.json")
    if not os.path.isfile(schema_path):
        pytest.skip(f"no schema for stream {stream_name}")
    fixture_filename, key_path = STREAM_FIXTURE_MAP[stream_name]
    fixture_path = os.path.join(_responses_dir(), fixture_filename)
    if not os.path.isfile(fixture_path):
        pytest.skip(f"no fixture {fixture_filename}")
    with open(schema_path) as f:
        schema = json.load(f)
    records = _get_records(fixture_path, key_path)
    if not records:
        pytest.skip(f"empty record list in {fixture_filename}")
    record = records[0]
    try:
        jsonschema.validate(instance=record, schema=schema)
    except jsonschema.ValidationError as e:
        pytest.fail(f"Fixture {fixture_filename} record does not match schema for {stream_name}: {e.message} at {e.absolute_path}")
