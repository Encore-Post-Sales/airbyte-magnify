# Jira Data Center source connector

This is the repository for the Jira Data Center source connector, written in Python.
For information about the Jira Data Center API, see [the Jira Data Center REST API documentation](https://developer.atlassian.com/server/jira/platform/rest/v10002/intro/#gettingstarted).

## Local development

### Prerequisites

- Python (~=3.9)
- Poetry (~=1.7) - installation instructions [here](https://python-poetry.org/docs/#installation)

### Installing the connector

From this connector directory, run:

```bash
poetry install --with dev
```

### Create credentials

**If you are a community contributor**, see the [Jira Data Center REST API documentation](https://developer.atlassian.com/server/jira/platform/rest/v10002/intro/#gettingstarted)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `source_jira_data_center/spec.json` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `sample_files/sample_config.json` for a sample config file.

### Locally running the connector

```
poetry run source-jira-data-center spec
poetry run source-jira-data-center check --config secrets/config.json
poetry run source-jira-data-center discover --config secrets/config.json
poetry run source-jira-data-center read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Running unit tests

To run unit tests locally, from the connector directory run:

```
poetry run pytest unit_tests
```

### Building the docker image

1. Install [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)
2. Run the following command to build the docker image:

```bash
airbyte-ci connectors --name=source-jira-data-center build
```

An image will be available on your host with the tag `airbyte/source-jira-data-center:dev`.

### Running as a docker container

Then run any of the connector commands as follows:

```
docker run --rm airbyte/source-jira-data-center:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-jira-data-center:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-jira-data-center:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-jira-data-center:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

### Running our CI test suite

You can run our full test suite locally using [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md):

```bash
airbyte-ci connectors --name=source-jira-data-center test
```

### Customizing acceptance Tests

Customize `acceptance-test-config.yml` file to configure acceptance tests. See [Connector Acceptance Tests](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference) for more information.
If your connector requires to create or destroy resources for use during acceptance tests create fixtures for it and place them inside integration_tests/acceptance.py.

### Dependency Management

All of your dependencies should be managed via Poetry.
To add a new dependency, run:

```bash
poetry add <package-name>
```

Please commit the changes to `pyproject.toml` and `poetry.lock` files.

### Issues stream and destination compatibility

The **issues** stream returns the full Jira issue search response from the **Jira Data Center REST API v2** (`/rest/api/2/search`). In v2, `fields.description` is a string (wiki markup or plain text), not Atlassian Document Format (ADF). If you sync to a destination that uses **Avro** and see conversion errors (e.g. `field fields is expected to be one of these: NULL, RECORD`):

1. Re-run **discover** so the catalog uses the latest stream schema.
2. If errors persist, the destination may require stricter types; consider excluding the issues stream or using a destination that supports flexible/JSON object types.

For response shapes, see [Jira Data Center REST API – search](https://developer.atlassian.com/server/jira/platform/rest/v10002/api-group-search/) and [Getting started](https://developer.atlassian.com/server/jira/platform/rest/v10002/intro/#gettingstarted).

### Schema v2 alignment

Stream schemas in `source_jira_data_center/schemas/` must align with the **Jira Data Center REST API v2** (`/rest/api/2/`), not Cloud v3. When editing or adding schemas, follow these rules:

| Area | Cloud v3 (avoid) | Data Center v2 rule |
|------|------------------|----------------------|
| **User identifiers** | `accountId` primary; `key`/`name` deprecated | Allow both; describe `key`/`name` as valid for v2; do not link to Cloud deprecation notice. |
| **Comment/description body** | ADF object only | Prefer `string` (wiki/plain) for v2; allow `object` only if a specific DC version returns ADF. |
| **Deprecation text** | "no longer available", link to Cloud deprecation notice | Remove or replace with "Optional; may be absent in some Data Center versions" and link to [Data Center REST API intro](https://developer.atlassian.com/server/jira/platform/rest/v10002/intro/). |
| **deploymentType** (server_info) | "always returned as *Cloud*" | Use "Server" or "Data Center" for this connector. |
| **External links** | `developer.atlassian.com/cloud/...` | Use `developer.atlassian.com/server/jira/platform/rest/v10002/...` where applicable. |

Reference: [Jira Data Center REST API](https://developer.atlassian.com/server/jira/platform/rest/v10002/intro/).

## Publishing a new version of the connector

You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?

1. Make sure your changes are passing our test suite: `airbyte-ci connectors --name=source-jira-data-center test`
2. Bump the connector version (please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors)):
   - bump the `dockerImageTag` value in in `metadata.yaml`
   - bump the `version` value in `pyproject.toml`
3. Make sure the `metadata.yaml` content is up to date.
4. Make sure the connector documentation and its changelog is up to date (`docs/integrations/sources/jira-data-center.md`).
5. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
6. Pat yourself on the back for being an awesome contributor.
7. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
8. Once your PR is merged, the new version of the connector will be automatically published to Docker Hub and our connector registry.
