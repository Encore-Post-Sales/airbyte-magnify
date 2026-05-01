#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Mapping, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .streams import (
    GainsightCsObjectStream
)
from .authenticator import GainsightCsAuthenticator

logger = logging.getLogger(__name__)

class SourceGainsightCs(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            authenticator = GainsightCsAuthenticator(config)
        except (KeyError, TypeError) as e:
            return False, f"Invalid configuration: {e}"
        logger.info(f"Checking connection to {authenticator.domain_url}")
        try:
            url = f"{authenticator.domain_url}/v1/meta/services/objects/Person/describe?idd=true"
            response = requests.get(url, auth=authenticator)
            if response.status_code == 200:
                return True, None
            else:
                return False, f"Failed to connect to API: {response.text}"
        except requests.exceptions.RequestException as e:
            return False, e

    def get_objects(self, config):
        authenticator = GainsightCsAuthenticator(config)
        url = f"{authenticator.domain_url}/v1/meta/services/objects"
        try:
            payload = {
                "externalUse": "true",
                "sortByLabel": "false"
            }
            session = requests.post(url, json=payload, auth=authenticator)
            body = session.json()
            data = body.get("data", [])
            return [obj["objectName"] for obj in data]
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = GainsightCsAuthenticator(config)
        all_objects = self.get_objects(config)
        lookback_days = config.get("lookback_730_day_streams", [])

        streams_by_name = {}
        for object_name in all_objects:
            streams_by_name[object_name] = GainsightCsObjectStream(
                name=object_name,
                authenticator=authenticator,
                lookback_730_day_streams=lookback_days,
            )

        authenticator._rotate()
        base_url = f"{authenticator.domain_url}/v1/meta/services/objects"

        def _fetch_describe(object_name: str):
            url = f"{base_url}/{object_name}/describe?idd=true"
            resp = requests.get(url, auth=authenticator)
            return object_name, resp.json()

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_fetch_describe, name): name for name in all_objects}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    _, body = future.result()
                    streams_by_name[name]._build_schema_from_describe_response(body)
                except Exception:
                    logger.warning("Parallel describe fetch failed for '%s'; will fall back to lazy fetch", name, exc_info=True)

        return list(streams_by_name.values())
