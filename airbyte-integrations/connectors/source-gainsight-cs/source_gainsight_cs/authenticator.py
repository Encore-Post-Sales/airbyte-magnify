import logging
import threading
import time
import requests
import base64

logger = logging.getLogger(__name__)


class GainsightCsAuthenticator(requests.auth.AuthBase):
    _token_request_path = '/v1/users/m2m/oauth/token'

    def __init__(self, config: dict):
        self._client_id = config["client_id"]
        self._client_secret = config["client_secret"]
        self._domain_url = config["domain_url"]

        self._token: dict = {}
        self._headers = {}
        self._token_acquired_at = None  # Time when token was fetched (in seconds)
        self._expires_in = None  # Token lifetime (in seconds)
        self._lock = threading.Lock()

    @property
    def domain_url(self):
        return self._domain_url

    def _is_token_expired(self) -> bool:
        # If no token or missing metadata, assume it's expired.
        if not self._token or self._token_acquired_at is None or self._expires_in is None:
            return True

        current_time = time.time()
        # Subtract a 60-second buffer to account for network delays.
        if current_time > self._token_acquired_at + self._expires_in - 60:
            return True

        return False

    def _rotate(self):
        with self._lock:
            if self._is_token_expired():
                logger.warning("Access token expired or missing, requesting a new token")
                try:
                    credentials = f"{self._client_id}:{self._client_secret}"
                    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
                    headers = {
                        "Authorization": f"Basic {encoded_credentials}",
                        "Content-Type": "application/json"
                    }
                    url = f"{self._domain_url}{self._token_request_path}"

                    response = requests.post(url=url, headers=headers)
                    if response.status_code != 200:
                        logger.error("Token request failed with status %d: %s", response.status_code, response.text)
                        raise Exception(f"Error fetching access token: {response.text}")

                    self._token = response.json()
                    self._token_acquired_at = time.time()
                    self._expires_in = self._token.get("expires_in", 0)
                    logger.warning(
                        "Access token refreshed successfully, expires in %ds (~%.1fh)",
                        self._expires_in,
                        self._expires_in / 3600,
                    )
                except requests.exceptions.RequestException as e:
                    logger.error("Network error while fetching access token: %s", e)
                    raise Exception(f"Error fetching access token: {e}") from e
            else:
                seconds_remaining = (self._token_acquired_at + self._expires_in - 60) - time.time()
                logger.debug("Access token still valid, %.1fs remaining before expiry buffer", seconds_remaining)

    def __call__(self, r: requests.Request) -> requests.Request:
        self._rotate()
        r.headers["Authorization"] = f"Bearer {self._token.get('access_token')}"
        return r

    def get_auth_header(self) -> dict:
        """
        Returns the authorization header with the current access token.
        """
        self._rotate()
        return {"Authorization": f"Bearer {self._token.get('access_token')}"}
