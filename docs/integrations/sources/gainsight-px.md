# Gainsight-API

This page contains the setup guide and reference information for the [Gainsight-PX-API](https://gainsightpx.docs.apiary.io/) source connector from [Gainsight-PX](https://support.gainsight.com/PX/API_for_Developers)

## Prerequisites

Api key is mandate for this connector to work, It could be generated from the dashboard settings (ref - https://app.aptrinsic.com/settings/api-keys).

## Setup guide

### Step 1: Set up Gainsight-API connection

- Generate an API key (Example: 12345)
- Params (If specific info is needed)
- Available configuration parameters:
  - api_key: The Aptrinsic API key (required)
  - start_date: Starting date for incremental data sync (optional, defaults to "2020-01-01T00:00:00.000Z")

## Step 2: Set up the Gainsight-APIs connector in Airbyte

### For Airbyte Cloud:

1. [Log into your Airbyte Cloud](https://cloud.airbyte.io/workspaces) account.
2. In the left navigation bar, click **Sources**. In the top-right corner, click **+new source**.
3. On the Set up the source page, enter the name for the Gainsight-API connector and select **Gainsight-API** from the Source type dropdown.
4. Enter your `api_key`.
5. (Optional) Enter a `start_date` to specify when incremental sync should begin. Format: YYYY-MM-DDTHH:mm:ss.SSSZ (e.g., "2020-01-01T00:00:00.000Z")
6. Click **Set up source**.

### For Airbyte OSS:

1. Navigate to the Airbyte Open Source dashboard.
2. Set the name for your source.
3. Enter your `api_key`.
4. (Optional) Enter a `start_date` to specify when incremental sync should begin. Format: YYYY-MM-DDTHH:mm:ss.SSSZ (e.g., "2020-01-01T00:00:00.000Z")
5. Click **Set up source**.

## Supported sync modes

The Gainsight-API source connector supports the following [sync modes](https://docs.airbyte.com/cloud/core-concepts#connection-sync-modes):

| Feature                       | Supported? |
| :---------------------------- | :--------- |
| Full Refresh Sync             | Yes        |
| Incremental Sync              | Yes        |
| Replicate Incremental Deletes | No         |
| SSL connection                | Yes        |
| Namespaces                    | No         |

### Incremental Sync Support

The following streams support incremental synchronization based on their timestamp fields:

| Stream                 | Cursor Field     | Description                                          |
| :--------------------- | :--------------- | :--------------------------------------------------- |
| accounts               | lastModifiedDate | Account data updated after the cursor date           |
| users                  | lastModifiedDate | User data updated after the cursor date              |
| articles               | modifiedDate     | Articles modified after the cursor date              |
| kcbot                  | modifiedDate     | Knowledge center bots modified after date            |
| email_events           | date             | Email events created after the cursor date           |
| engagement_view_events | date             | Engagement view events created after the cursor date |
| feature_match_events   | date             | Feature match events created after the cursor date   |
| form_submit_events     | date             | Form submit events created after the cursor date     |
| identify_events        | date             | Identify events created after the cursor date        |
| page_view_events       | date             | Page view events created after the cursor date       |
| session_events         | date             | Session events created after the cursor date         |

**Note:** When using incremental sync, you can specify a `start_date` in the connector configuration to define the initial sync point.

## Supported Streams

### Core Entity Streams

- **accounts** - Account information and metadata
- **admin_attributes** - Administrative attributes configuration
- **articles** - Knowledge base articles and content
- **feature** - Feature definitions and configurations
- **kcbot** - Knowledge center bot configurations
- **segments** - User segmentation definitions
- **user_attributes** - User attribute definitions
- **users** - User profiles and information

### Event Streams

- **email_events** - Email interaction and engagement events
- **engagement_view_events** - In-app engagement view events
- **feature_match_events** - Feature matching and targeting events
- **form_submit_events** - Form submission events
- **identify_events** - User identification and profile events
- **page_view_events** - Page view and navigation events
- **session_events** - Session start/end and lifecycle events

## API method example

GET https://api.aptrinsic.com/v1/accounts

## Performance considerations

Gainsight-PX-API's [API reference](https://gainsightpx.docs.apiary.io/) has v1 at present. The connector as default uses v1.

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request                                             | Subject                                    |
| :------ | :--------- | :------------------------------------------------------- | :----------------------------------------- |
| 0.2.0   | 2024-08-19 | [44414](https://github.com/airbytehq/airbyte/pull/44414) | Refactor connector to manifest-only format |
| 0.1.14  | 2024-08-17 | [44248](https://github.com/airbytehq/airbyte/pull/44248) | Update dependencies                        |
| 0.1.13  | 2024-08-12 | [43902](https://github.com/airbytehq/airbyte/pull/43902) | Update dependencies                        |
| 0.1.12  | 2024-08-10 | [43117](https://github.com/airbytehq/airbyte/pull/43117) | Update dependencies                        |
| 0.1.11  | 2024-07-27 | [42732](https://github.com/airbytehq/airbyte/pull/42732) | Update dependencies                        |
| 0.1.10  | 2024-07-20 | [42182](https://github.com/airbytehq/airbyte/pull/42182) | Update dependencies                        |
| 0.1.9   | 2024-07-13 | [41928](https://github.com/airbytehq/airbyte/pull/41928) | Update dependencies                        |
| 0.1.8   | 2024-07-10 | [41365](https://github.com/airbytehq/airbyte/pull/41365) | Update dependencies                        |
| 0.1.7   | 2024-07-09 | [41075](https://github.com/airbytehq/airbyte/pull/41075) | Update dependencies                        |
| 0.1.6   | 2024-07-06 | [40893](https://github.com/airbytehq/airbyte/pull/40893) | Update dependencies                        |
| 0.1.5   | 2024-06-25 | [40352](https://github.com/airbytehq/airbyte/pull/40352) | Update dependencies                        |
| 0.1.4   | 2024-06-22 | [39988](https://github.com/airbytehq/airbyte/pull/39988) | Update dependencies                        |
| 0.1.3   | 2024-06-04 | [38979](https://github.com/airbytehq/airbyte/pull/38979) | [autopull] Upgrade base image to v1.2.1    |
| 0.1.2   | 2024-05-28 | [38669](https://github.com/airbytehq/airbyte/pull/38669) | Make connector compatible with Builder     |
| 0.1.1   | 2024-05-03 | [37593](https://github.com/airbytehq/airbyte/pull/37593) | Changed `last_records` to `last_record`    |
| 0.1.0   | 2023-05-10 | [26998](https://github.com/airbytehq/airbyte/pull/26998) | Initial PR                                 |

</details>
