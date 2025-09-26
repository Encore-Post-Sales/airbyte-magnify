#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import hashlib
import hmac
import base64
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Mapping, Union

import requests
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import NoAuth
from airbyte_cdk.sources.declarative.interpolation import InterpolatedString
from airbyte_cdk.sources.declarative.types import Config


@dataclass
class CounselLinkAuthenticator(NoAuth):
    """
    Custom authenticator for CounselLink API that implements HMAC-SHA256 signature authentication.
    
    Based on the authentication method used by CounselLink API:
    1. Creates a string to sign containing method, content type, date, and URI
    2. Generates HMAC-SHA256 signature using the secret key
    3. Base64 encodes the signature
    4. Adds required headers to the request
    """
    
    config: Config
    access_key: Union[InterpolatedString, str]
    secret_key: Union[InterpolatedString, str]
    server: Union[InterpolatedString, str]

    def __post_init__(self, parameters: Mapping[str, Any]):
        self._access_key = InterpolatedString.create(self.access_key, parameters=parameters).eval(self.config)
        self._secret_key = InterpolatedString.create(self.secret_key, parameters=parameters).eval(self.config)
        self._server = InterpolatedString.create(self.server, parameters=parameters).eval(self.config)
        self.content_type = 'application/vnd.cl.int.api.v1.0.0+json'

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        """
        Add CounselLink authentication headers to the request.
        """
        # Generate ISO 8601 date string
        iso8601_date = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Extract request method and URI
        request_method = request.method or "GET"
        request_uri = request.path_url or "/"
        
        # Generate signature
        signature = self._generate_signature(request_method, request_uri, iso8601_date)
        
        # Create authorization header
        authorization = f"CL-INT-API {self._access_key}:{signature}"
        
        # Add required headers
        auth_headers = {
            'Authorization': authorization,
            'x-ln-cl-int-api-date': iso8601_date,
            'x-ln-cl-int-api-contentmd5': '',  # Empty for GET requests
            'Content-Type': self.content_type,
            'client_key': self._access_key,
            'Accept': self.content_type
        }
        
        # Update request headers
        if request.headers is None:
            request.headers = {}
        request.headers.update(auth_headers)
        
        return request

    def _generate_signature(self, request_method: str, request_uri: str, iso8601_date: str) -> str:
        """
        Generate HMAC-SHA256 signature for CounselLink API authentication.
        
        For GET requests, the string to sign format is:
        {method}\n\n\n{iso8601_date}\n{request_uri}\n
        
        For other methods, it includes content-type and content-md5:
        {method}\n\n\n{content_type}\n{content_md5}\n{iso8601_date}\n{request_uri}\n
        """
        try:
            if request_method != 'GET':
                # For non-GET requests, include content MD5
                md5 = hashlib.md5("RANDOM_STRING".encode('utf-8')).digest()
                content_md5 = base64.b64encode(md5).decode()
                string_to_sign = f"{request_method}\n\n\n{self.content_type}\n{content_md5}\n{iso8601_date}\n{request_uri}\n"
            else:
                # For GET requests, simpler format
                string_to_sign = f"{request_method}\n\n\n{iso8601_date}\n{request_uri}\n"
            
            # Generate HMAC signature
            string_to_sign_bytes = string_to_sign.encode('utf-8')
            secret_key_bytes = self._secret_key.encode('utf-8')
            hmac_hash = hmac.new(secret_key_bytes, string_to_sign_bytes, hashlib.sha256).hexdigest()
            
            # Base64 encode the signature
            signature = base64.b64encode(bytes.fromhex(hmac_hash)).decode('utf-8')
            
            return signature
            
        except Exception as e:
            raise Exception(f"Error generating CounselLink signature: {e}") from e

    @property
    def token(self) -> str:
        """Return the access key as the token."""
        return self._access_key
