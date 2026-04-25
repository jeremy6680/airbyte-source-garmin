"""
Garmin Connect authentication with session persistence and retry logic.

Authentication flow on each connector run:
  1. If a session file exists, load it and validate with a lightweight API call.
     On success → return the client immediately, no SSO login needed.
  2. If no file or session is expired → perform a fresh SSO login via
     garminconnect.Garmin.login().
  3. After a successful fresh login, persist the token via garth.dump() so the
     next run can skip the login.
  4. On HTTP 429 (rate limit) → retry up to 3 times with delays of 30s, 60s, 120s.
"""

import os
from typing import Optional

import garminconnect
from loguru import logger

from source_garmin.config import ConnectorConfig
from source_garmin.utils import _RETRY_DELAYS, retry_on_429


class GarminAuth:
    """Handles Garmin Connect authentication with session caching and retry.

    Garmin has no official API and rate-limits SSO logins aggressively.
    This class minimises login frequency by persisting OAuth tokens to a file
    between runs and only performing a fresh login when the token has expired.

    Attributes:
        config: The validated connector configuration.
        _client: Cached garminconnect.Garmin instance, populated on first call
            to get_client().
    """

    def __init__(self, config: ConnectorConfig) -> None:
        """Initialise with a validated connector config.

        Args:
            config: Validated ConnectorConfig instance from config.py.
        """
        self.config = config
        self._client: Optional[garminconnect.Garmin] = None

    def get_client(self) -> garminconnect.Garmin:
        """Return an authenticated Garmin Connect client.

        Lazy-initialises on first call; returns the cached instance on
        subsequent calls within the same connector run.

        Returns:
            An authenticated garminconnect.Garmin instance ready for API calls.

        Raises:
            garminconnect.GarminConnectAuthenticationError: If credentials are
                invalid.
            garminconnect.GarminConnectTooManyRequestsError: If the rate limit
                persists after all retry attempts are exhausted.
        """
        if self._client is None:
            self._client = self._authenticate()
        return self._client

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _authenticate(self) -> garminconnect.Garmin:
        """Orchestrate the full authentication flow.

        Tries session restore first; falls back to a fresh login if needed.

        Returns:
            An authenticated garminconnect.Garmin instance.
        """
        # Instantiate the client with credentials.
        # get_secret_value() is required because password is a SecretStr.
        client = garminconnect.Garmin(
            email=self.config.email,
            password=self.config.password.get_secret_value(),
        )

        if self._try_load_session(client):
            return client

        logger.info("No valid cached session — performing fresh Garmin SSO login.")
        self._login_with_retry(client)
        return client

    def _try_load_session(self, client: garminconnect.Garmin) -> bool:
        """Attempt to restore a previously persisted OAuth session from disk.

        Loads tokens via garth.load() then immediately validates them with a
        lightweight API call. Returns False instead of raising so that
        _authenticate() can silently fall back to a fresh login.

        Args:
            client: An uninitialised garminconnect.Garmin instance.

        Returns:
            True if the session file exists and the token is still valid;
            False otherwise.
        """
        session_path = self.config.session_file_path

        if not os.path.exists(session_path):
            logger.debug("No session file found at {} — skipping restore.", session_path)
            return False

        try:
            # Deserialise the OAuth token from JSON.
            # Does NOT make a network call — the token could still be expired.
            client.client.load(session_path)

            # Fetch the social profile: validates the token with a real network call
            # AND populates client.display_name / client.full_name, which are required
            # by endpoints like get_user_summary() that call _require_display_name().
            # Any expired or revoked token raises an exception here.
            profile = client.connectapi("/userprofile-service/socialProfile")
            client.display_name = profile.get("displayName", client.username)
            client.full_name = profile.get("fullName")

            logger.info("Restored valid Garmin session for {}.", client.display_name)
            return True

        except Exception as exc:
            # Broad catch: garminconnect can raise several exception types for an
            # expired session (GarminConnectAuthenticationError, HTTPError, etc.).
            # In all cases the right action is the same: fall back to fresh login.
            logger.warning(
                "Cached session at {} is invalid or expired ({}). Will re-login.",
                session_path,
                exc,
            )
            return False

    def _login_with_retry(self, client: garminconnect.Garmin) -> None:
        """Perform a fresh SSO login, retrying on HTTP 429 rate-limit errors.

        Delegates retry logic to retry_on_429() from utils.py so the backoff
        schedule is defined and tested in one place.  Raises immediately on
        authentication errors — retrying a bad password is pointless.

        After a successful login, persists the token to disk so the next run can
        skip the SSO flow entirely.

        Args:
            client: An uninitialised garminconnect.Garmin instance.

        Raises:
            garminconnect.GarminConnectAuthenticationError: On invalid credentials.
            garminconnect.GarminConnectTooManyRequestsError: When all retry
                attempts are exhausted.
        """
        try:
            retry_on_429(client.login)
        except garminconnect.GarminConnectAuthenticationError:
            # Wrong credentials — retrying will never fix this.
            logger.error("Garmin authentication failed: invalid email or password.")
            raise
        # GarminConnectTooManyRequestsError propagates from retry_on_429 as-is.

        self._save_session(client)
        logger.info("Garmin login successful.")

    def _save_session(self, client: garminconnect.Garmin) -> None:
        """Persist the OAuth token to the configured session file.

        Creates any missing parent directories so the path works out-of-the-box
        in both local dev and Docker environments.

        Failure is logged as a warning rather than an exception: the sync can
        still complete without persistence; the next run will simply need to
        re-login.

        Args:
            client: An authenticated garminconnect.Garmin instance.
        """
        session_path = self.config.session_file_path
        parent_dir = os.path.dirname(session_path) or "."
        os.makedirs(parent_dir, exist_ok=True)

        try:
            client.client.dump(session_path)
            logger.info("Garmin session token saved to {}.", session_path)
        except OSError as exc:
            logger.warning(
                "Could not save session token to {} ({}). "
                "Next run will require a fresh login.",
                session_path,
                exc,
            )
