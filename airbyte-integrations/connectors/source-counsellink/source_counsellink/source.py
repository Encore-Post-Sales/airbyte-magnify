#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import hashlib
import hmac
import base64
from datetime import datetime, timezone
import logging
import csv
import os

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator

logger = logging.getLogger(__name__)

class CounselLinkAuthenticator:
    """
    Custom authenticator for CounselLink API using HMAC-SHA256 signature authentication.
    This generates authentication headers that can be used by multiple streams.
    """
    
    def __init__(self, access_key: str, secret_key: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.content_type = 'application/vnd.cl.int.api.v1.0.0+json'

    def get_auth_headers(self, request_path: str, request_method: str = "GET") -> Mapping[str, Any]:
        """
        Generate authentication headers for CounselLink API requests.
        
        Args:
            request_path: The API path (e.g., "user/login-audit")
            request_method: HTTP method (default: "GET")
            
        Returns:
            Dictionary containing all required authentication headers
        """
        # Generate ISO8601 timestamp
        iso8601_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Ensure request_uri starts with slash
        request_uri = "/" + request_path.lstrip("/")
        
        # Create string to sign (matching working example format)
        string_to_sign = f"{request_method}\n\n\n{iso8601_date}\n{request_uri}\n"
        
        # Generate HMAC-SHA256 signature
        string_to_sign_bytes = string_to_sign.encode('utf-8')
        secret_key_bytes = self.secret_key.encode('utf-8')
        hmac_hash = hmac.new(secret_key_bytes, string_to_sign_bytes, hashlib.sha256).hexdigest()
        signature = base64.b64encode(bytes.fromhex(hmac_hash)).decode('utf-8')
        
        # Create authorization header
        authorization = f"CL-INT-API {self.access_key}:{signature}"
        
        # Return all required headers
        return {
            'Authorization': authorization,
            'x-ln-cl-int-api-date': iso8601_date,
            'x-ln-cl-int-api-contentmd5': '',
            'Content-Type': self.content_type,
            'client_key': self.access_key
        }


# Basic full refresh stream
class CounsellinkStream(HttpStream, ABC):
    """
    Base stream class for CounselLink API.
    Contains common functionality like authentication, base URL, and response parsing.
    """

    def __init__(self, config: Mapping[str, Any], counsellink_authenticator: CounselLinkAuthenticator, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.counsellink_authenticator = counsellink_authenticator

    @property
    def url_base(self) -> str:
        """
        Override url_base as a property to use the configured server.
        """
        server = self.config.get("server", "api-cve11.counsellink.net")
        return f"https://{server}/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        CounselLink API doesn't appear to have built-in pagination for the login-audit endpoint.
        Returns None to indicate no pagination.
        """
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        Base request parameters. Individual streams can override this for specific params.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Parse the JSON response from CounselLink API.
        Expects the response to be a JSON array of objects.
        """
        json_response = response.json()
        if isinstance(json_response, list):
            for record in json_response:
                yield record
        else:
            # Handle single object responses
            yield json_response

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Add CounselLink authentication headers using the authenticator.
        This method is common to all CounselLink streams.
        """
        request_path = self.path(stream_state, stream_slice, next_page_token)
        return self.counsellink_authenticator.get_auth_headers(request_path)


class MatterGuidStream(CounsellinkStream):
    """
    Base stream class for CounselLink streams that need to iterate through matter GUIDs.
    Provides common functionality for loading GUIDs from CSV and stream slicing.
    """
    
    def __init__(self, config: Mapping[str, Any], counsellink_authenticator: CounselLinkAuthenticator, **kwargs):
        super().__init__(config, counsellink_authenticator, **kwargs)
        self.matter_guids = self._load_matter_guids()
    
    def _load_matter_guids(self) -> List[str]:
        """
        Load matter GUIDs from the matter_guid.csv file.
        Returns a list of GUIDs read from the CSV file.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_file_path = os.path.join(current_dir, 'matter_guid.csv')
        
        matter_guids = []
        try:
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                # The CSV file has one GUID per line, no headers
                csv_reader = csv.reader(csvfile)
                for row in csv_reader:
                    if row and row[0].strip():  # Skip empty rows
                        matter_guids.append(row[0].strip())
            
            logger.info(f"Loaded {len(matter_guids)} matter GUIDs from {csv_file_path}")
            return matter_guids
        except FileNotFoundError:
            logger.error(f"Matter GUID CSV file not found at {csv_file_path}")
            raise FileNotFoundError(f"Matter GUID CSV file not found at {csv_file_path}")
        except Exception as e:
            logger.error(f"Error reading matter GUID CSV file: {e}")
            raise Exception(f"Error reading matter GUID CSV file: {e}")
    
    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Generate stream slices for each matter GUID.
        Each slice represents one API call for a single GUID.
        """
        for matter_guid in self.matter_guids:
            yield {"matter_guid": matter_guid}


class UserLoginAuditStream(CounsellinkStream):
    """
    Stream for fetching user login audit data from CounselLink API.
    """

    # Primary key based on the schema - loginId should be unique
    primary_key = "loginId"
    
    # Explicitly set the stream name to match our configured catalog
    name = "user_login_audit"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        Returns the API path for the login audit endpoint.
        """
        return "user/login-audit"

class MatterStream(MatterGuidStream):
    """
    Stream for fetching matter data from CounselLink API.
    Each API call handles one GUID at a time using stream slicing.
    """
    
    # Primary key based on the schema - guid should be unique
    primary_key = "guid"
    
    # Explicitly set the stream name to match our configured catalog
    name = "matter"
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None,
    ) -> str:
        """
        Returns the API path for the matter endpoint with a single GUID.
        """
        if stream_slice:
            matter_guid = stream_slice["matter_guid"]
        else:
            # Fallback to first GUID if no slice provided
            matter_guid = self.matter_guids[0]
        
        return f"matter/{matter_guid}"

class MatterParticipantStream(MatterGuidStream): 
    """
    Stream for fetching matter participant data from CounselLink API.
    """
    
    # Primary key based on the schema - guid should be unique
    primary_key = "contactId"

    # Explicitly set the stream name to match our configured catalog
    name = "matter_participant"
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        Returns the API path for the matter participant endpoint.
        """
        matter_guid = stream_slice["matter_guid"]
        
        return f"matter/{matter_guid}/participants"

class JournalEntryStream(MatterGuidStream):
    """
    Stream for fetching journal entry data from CounselLink API.
    """
    
    # Primary key based on the schema - id should be unique
    primary_key = "id"
    
    # Explicitly set the stream name to match our configured catalog
    name = "journal_entry"
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        Returns the API path for the journal entry endpoint.
        """
        matter_guid = stream_slice["matter_guid"]
        return f"matter/{matter_guid}/journalEntries"


# Source
class SourceCounsellink(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Test connection to CounselLink API by making a test request to the login-audit endpoint.
        
        :param config: the user-input config object conforming to the connector's spec.yaml
        :param logger: logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            # Create authenticator
            authenticator = CounselLinkAuthenticator(
                access_key=config["access_key"],
                secret_key=config["secret_key"]
            )
            
            # Create a test stream to verify connection
            test_stream = UserLoginAuditStream(config=config, counsellink_authenticator=authenticator)
            
            # Try to read the first record to test the connection
            records = test_stream.read_records(sync_mode=None)
            next(records, None)  # Try to get first record
            
            return True, None
            
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Return list of available streams for CounselLink connector.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # Create shared authenticator for all streams
        authenticator = CounselLinkAuthenticator(
            access_key=config["access_key"],
            secret_key=config["secret_key"]
        )
        
        return [
            UserLoginAuditStream(config=config, counsellink_authenticator=authenticator),
            MatterStream(config=config, counsellink_authenticator=authenticator),
            MatterParticipantStream(config=config, counsellink_authenticator=authenticator),
            JournalEntryStream(config=config, counsellink_authenticator=authenticator)
        ]
