# CounselLink source connector

This directory contains the manifest-only connector for `source-counsellink`.
This connector implements custom HMAC-SHA256 authentication for the CounselLink API.

For information about how to configure and use this connector within Airbyte, see [the connector's full documentation](https://docs.airbyte.com/integrations/sources/counsellink).

## Local development

We recommend using the Connector Builder to edit this connector.
Using either Airbyte Cloud or your local Airbyte OSS instance, navigate to the **Builder** tab and select **Import a YAML**.
Then select the connector's `manifest.yaml` file to load the connector into the Builder. You're now ready to make changes to the connector!

If you prefer to develop locally, you can follow the instructions below.

### Building the docker image

You can build any manifest-only connector with `airbyte-ci`:

1. Install [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)
2. Run the following command to build the docker image:

```bash
airbyte-ci connectors --name=source-counsellink build
```

An image will be available on your host with the tag `airbyte/source-counsellink:dev`.

### Creating credentials

**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/counsellink)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `spec` object in the connector's `manifest.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.

### Running as a docker container

Then run any of the standard source connector commands:

```bash
docker run --rm airbyte/source-counsellink:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-counsellink:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-counsellink:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-counsellink:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

## Configuration

### Authentication

This connector uses CounselLink's custom HMAC-SHA256 signature authentication. You need:

- **Server**: Your CounselLink API server hostname (e.g., `api-cve11.counsellink.net`)
- **Access Key**: Your CounselLink API access key
- **Secret Key**: Your CounselLink API secret key

### Data Configuration

- **Start Date**: Start date for data sync (optional)
- **End Date**: End date for data sync (optional)

## Supported Streams

1. **Client Contacts**: All client contact information from the CounselLink system

## Custom Authentication Implementation

This connector implements a custom authenticator (`CounselLinkAuthenticator`) that:

1. Generates ISO 8601 timestamps for each request
2. Creates HMAC-SHA256 signatures using the provided secret key
3. Adds the required CounselLink headers to each API request

The authentication follows CounselLink's specification exactly as implemented in their API documentation.
