"""
Unit tests for source_garmin/auth.py (GarminAuth).

Strategy:
  - garminconnect.Garmin is never instantiated for real — network calls are fully
    avoided by passing MagicMock clients directly to private methods, or by patching
    the constructor in tests that exercise the full _authenticate() flow.
  - time.sleep is patched throughout to keep the suite fast (no 30-120 s waits).
  - Private methods (_try_load_session, _login_with_retry, _save_session) are tested
    in isolation to make failures easy to pinpoint.
"""

from unittest.mock import MagicMock, patch

import garminconnect
import pytest

from source_garmin.auth import GarminAuth
from source_garmin.utils import _RETRY_DELAYS
from source_garmin.config import ConnectorConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_config(**overrides) -> ConnectorConfig:
    """Build a minimal valid ConnectorConfig.

    Args:
        **overrides: Any ConnectorConfig field to override from the defaults.

    Returns:
        A validated ConnectorConfig instance safe for unit tests.
    """
    defaults = {
        "email": "test@example.com",
        "password": "s3cr3t",
        "session_file_path": "/tmp/test_session.json",
    }
    defaults.update(overrides)
    return ConnectorConfig(**defaults)


# ---------------------------------------------------------------------------
# get_client() — lazy initialisation and caching
# ---------------------------------------------------------------------------

class TestGetClient:
    """get_client() should authenticate on the first call and return cached instance."""

    def test_triggers_authentication_on_first_call(self):
        """_authenticate() is called exactly once when _client is None."""
        auth = GarminAuth(make_config())
        mock_client = MagicMock()

        with patch.object(auth, "_authenticate", return_value=mock_client) as mock_auth:
            result = auth.get_client()

        mock_auth.assert_called_once()
        assert result is mock_client

    def test_returns_cached_client_on_subsequent_calls(self):
        """_authenticate() is NOT called again after the first get_client()."""
        auth = GarminAuth(make_config())
        mock_client = MagicMock()

        with patch.object(auth, "_authenticate", return_value=mock_client) as mock_auth:
            first = auth.get_client()
            second = auth.get_client()

        # Two calls to get_client() must trigger _authenticate() only once.
        mock_auth.assert_called_once()
        assert first is second


# ---------------------------------------------------------------------------
# _try_load_session()
# ---------------------------------------------------------------------------

class TestTryLoadSession:
    """_try_load_session() silently restores a cached OAuth session from disk."""

    def test_returns_false_when_session_file_missing(self, tmp_path):
        """Returns False without calling garth.load() when the file does not exist."""
        config = make_config(session_file_path=str(tmp_path / "no_such_file.json"))
        auth = GarminAuth(config)
        mock_client = MagicMock()

        result = auth._try_load_session(mock_client)

        assert result is False
        mock_client.garth.load.assert_not_called()

    def test_returns_true_for_valid_cached_session(self, tmp_path):
        """Returns True when the file exists and the token validation call succeeds."""
        session_file = tmp_path / "session.json"
        session_file.write_text("{}")  # file must physically exist

        config = make_config(session_file_path=str(session_file))
        auth = GarminAuth(config)

        mock_client = MagicMock()
        mock_client.get_full_name.return_value = "Jane Doe"

        result = auth._try_load_session(mock_client)

        assert result is True
        mock_client.garth.load.assert_called_once_with(str(session_file))
        mock_client.get_full_name.assert_called_once()

    def test_returns_false_when_token_is_expired(self, tmp_path):
        """Returns False (no exception to the caller) when the validation call fails."""
        session_file = tmp_path / "session.json"
        session_file.write_text("{}")

        config = make_config(session_file_path=str(session_file))
        auth = GarminAuth(config)

        mock_client = MagicMock()
        # Simulates an expired or revoked OAuth token.
        mock_client.get_full_name.side_effect = Exception("401 Unauthorized")

        result = auth._try_load_session(mock_client)

        assert result is False

    def test_returns_false_when_garth_load_raises(self, tmp_path):
        """Returns False even when garth.load() itself raises (corrupted file etc.)."""
        session_file = tmp_path / "session.json"
        session_file.write_text("not valid json")

        config = make_config(session_file_path=str(session_file))
        auth = GarminAuth(config)

        mock_client = MagicMock()
        mock_client.garth.load.side_effect = ValueError("cannot deserialise token")

        result = auth._try_load_session(mock_client)

        assert result is False


# ---------------------------------------------------------------------------
# _login_with_retry()
# ---------------------------------------------------------------------------

class TestLoginWithRetry:
    """_login_with_retry() performs SSO login with exponential back-off on HTTP 429."""

    @patch("source_garmin.utils.time.sleep")
    def test_succeeds_on_first_attempt_without_sleeping(self, mock_sleep, tmp_path):
        """No sleep when login succeeds immediately."""
        config = make_config(session_file_path=str(tmp_path / "session.json"))
        auth = GarminAuth(config)
        mock_client = MagicMock()

        auth._login_with_retry(mock_client)

        mock_client.login.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("source_garmin.utils.time.sleep")
    def test_raises_immediately_on_invalid_credentials(self, mock_sleep):
        """GarminConnectAuthenticationError is re-raised immediately, never retried.

        Retrying a wrong password is pointless and would lock the account faster.
        """
        auth = GarminAuth(make_config())
        mock_client = MagicMock()
        mock_client.login.side_effect = garminconnect.GarminConnectAuthenticationError(
            "bad credentials"
        )

        with pytest.raises(garminconnect.GarminConnectAuthenticationError):
            auth._login_with_retry(mock_client)

        mock_client.login.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("source_garmin.utils.time.sleep")
    def test_retries_after_429_and_succeeds(self, mock_sleep, tmp_path):
        """Waits the first retry delay, then succeeds on the second attempt."""
        config = make_config(session_file_path=str(tmp_path / "session.json"))
        auth = GarminAuth(config)
        mock_client = MagicMock()
        mock_client.login.side_effect = [
            garminconnect.GarminConnectTooManyRequestsError("rate limited"),
            None,  # success on the second call
        ]

        auth._login_with_retry(mock_client)

        assert mock_client.login.call_count == 2
        # Must sleep exactly once, with the first retry delay.
        mock_sleep.assert_called_once_with(_RETRY_DELAYS[0])

    @patch("source_garmin.utils.time.sleep")
    def test_raises_after_all_retry_attempts_exhausted(self, mock_sleep):
        """Raises GarminConnectTooManyRequestsError when every attempt is rate-limited."""
        auth = GarminAuth(make_config())
        mock_client = MagicMock()
        mock_client.login.side_effect = garminconnect.GarminConnectTooManyRequestsError(
            "still rate limited"
        )

        with pytest.raises(garminconnect.GarminConnectTooManyRequestsError):
            auth._login_with_retry(mock_client)

        # Attempts == number of retry delays defined in the module constant.
        assert mock_client.login.call_count == len(_RETRY_DELAYS)
        # Sleeps between attempts — one less sleep than total attempts.
        assert mock_sleep.call_count == len(_RETRY_DELAYS) - 1

    @patch("source_garmin.utils.time.sleep")
    def test_sleep_durations_match_retry_delays_constant(self, mock_sleep, tmp_path):
        """Sleep is called with the exact values from _RETRY_DELAYS, in order."""
        config = make_config(session_file_path=str(tmp_path / "session.json"))
        auth = GarminAuth(config)
        mock_client = MagicMock()

        # Fail on every attempt except the last one.
        side_effects = [
            garminconnect.GarminConnectTooManyRequestsError("429")
            for _ in range(len(_RETRY_DELAYS) - 1)
        ] + [None]
        mock_client.login.side_effect = side_effects

        auth._login_with_retry(mock_client)

        actual_sleep_args = [call_args[0][0] for call_args in mock_sleep.call_args_list]
        # Each sleep duration must match the corresponding entry in _RETRY_DELAYS.
        assert actual_sleep_args == _RETRY_DELAYS[: len(_RETRY_DELAYS) - 1]


# ---------------------------------------------------------------------------
# _save_session()
# ---------------------------------------------------------------------------

class TestSaveSession:
    """_save_session() persists the OAuth token and handles I/O failures gracefully."""

    def test_calls_garth_dump_with_configured_path(self, tmp_path):
        """garth.dump() is invoked with the session path from the config."""
        session_file = tmp_path / "session.json"
        config = make_config(session_file_path=str(session_file))
        auth = GarminAuth(config)
        mock_client = MagicMock()

        auth._save_session(mock_client)

        mock_client.garth.dump.assert_called_once_with(str(session_file))

    def test_creates_missing_parent_directories(self, tmp_path):
        """Parent directories are created automatically (important for Docker volumes)."""
        session_file = tmp_path / "subdir" / "nested" / "session.json"
        config = make_config(session_file_path=str(session_file))
        auth = GarminAuth(config)
        mock_client = MagicMock()

        auth._save_session(mock_client)

        assert session_file.parent.exists()

    def test_does_not_raise_on_oserror(self, tmp_path):
        """OSError from garth.dump() is swallowed — sync can proceed without caching.

        Failing to save the session must never abort a sync that is otherwise working.
        """
        session_file = tmp_path / "session.json"
        config = make_config(session_file_path=str(session_file))
        auth = GarminAuth(config)
        mock_client = MagicMock()
        mock_client.garth.dump.side_effect = OSError("disk full")

        # Should complete without raising.
        auth._save_session(mock_client)
