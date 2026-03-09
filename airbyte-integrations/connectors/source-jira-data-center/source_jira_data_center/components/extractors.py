# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from dataclasses import dataclass
from typing import Any, List, Mapping

from airbyte_cdk.sources.declarative.extractors import DpathExtractor
from requests_cache import Response


@dataclass
class ArrayOrValuesExtractor(DpathExtractor):
    """
    Extractor for Jira Data Center v2 API compatibility.
    Handles both response shapes:
    - Data Center GET /rest/api/2/priority and /resolution return a direct array.
    - Cloud/search endpoints return {"values": [...]}.
    Returns the list of records from either shape.
    """

    def extract_records(self, response: Response) -> List[Mapping[str, Any]]:
        body = response.json() if hasattr(response, "json") and callable(response.json) else response
        if isinstance(body, list):
            return body
        if isinstance(body, dict) and "values" in body:
            return body["values"]
        return []


@dataclass
class LabelsRecordExtractor(DpathExtractor):
    """
    A custom record extractor is needed to handle cases when records are represented as list of strings insted of dictionaries.
    Example:
        -> ["label 1", "label 2", ..., "label n"]
        <- [{"label": "label 1"}, {"label": "label 2"}, ..., {"label": "label n"}]
    """

    def extract_records(self, response: Response) -> List[Mapping[str, Any]]:
        records = super().extract_records(response)
        return [{"label": record} for record in records]
