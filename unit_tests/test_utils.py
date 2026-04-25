"""
Unit tests for source_garmin/utils.py (retry_on_429).

Strategy:
  - time.sleep is always patched to keep the suite fast (no real waits).
  - garminconnect.GarminConnectTooManyRequestsError is raised via side_effect
    to simulate rate-limiting without any network calls.
  - Non-429 exceptions are tested to verify they propagate immediately.
"""

from unittest.mock import MagicMock, call, patch

import garminconnect
import pytest

from source_garmin.utils import _RETRY_DELAYS, retry_on_429


class TestRetryOn429:
    """retry_on_429() wraps a callable and retries only on HTTP 429."""

    @patch("source_garmin.utils.time.sleep")
    def test_returns_value_on_first_success(self, mock_sleep):
        """Returns the callable's return value when the first attempt succeeds."""
        fn = MagicMock(return_value={"data": 42})

        result = retry_on_429(fn)

        assert result == {"data": 42}
        fn.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("source_garmin.utils.time.sleep")
    def test_retries_on_429_and_succeeds(self, mock_sleep):
        """Waits _RETRY_DELAYS[0] seconds then returns on the second attempt."""
        fn = MagicMock(
            side_effect=[
                garminconnect.GarminConnectTooManyRequestsError("429"),
                ["record_1", "record_2"],
            ]
        )

        result = retry_on_429(fn)

        assert result == ["record_1", "record_2"]
        assert fn.call_count == 2
        mock_sleep.assert_called_once_with(_RETRY_DELAYS[0])

    @patch("source_garmin.utils.time.sleep")
    def test_retries_twice_and_succeeds_on_third(self, mock_sleep):
        """Sleeps twice with the correct delays, succeeds on the third attempt."""
        fn = MagicMock(
            side_effect=[
                garminconnect.GarminConnectTooManyRequestsError("429"),
                garminconnect.GarminConnectTooManyRequestsError("429"),
                {"ok": True},
            ]
        )

        result = retry_on_429(fn)

        assert result == {"ok": True}
        assert fn.call_count == 3
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list == [call(_RETRY_DELAYS[0]), call(_RETRY_DELAYS[1])]

    @patch("source_garmin.utils.time.sleep")
    def test_raises_429_after_all_retries_exhausted(self, mock_sleep):
        """Re-raises GarminConnectTooManyRequestsError when every attempt fails."""
        fn = MagicMock(
            side_effect=garminconnect.GarminConnectTooManyRequestsError("still 429")
        )

        with pytest.raises(garminconnect.GarminConnectTooManyRequestsError):
            retry_on_429(fn)

        assert fn.call_count == len(_RETRY_DELAYS)
        # One sleep between each consecutive attempt.
        assert mock_sleep.call_count == len(_RETRY_DELAYS) - 1

    @patch("source_garmin.utils.time.sleep")
    def test_sleep_durations_match_retry_delays_in_order(self, mock_sleep):
        """Each sleep call uses the corresponding _RETRY_DELAYS value."""
        fn = MagicMock(
            side_effect=[
                garminconnect.GarminConnectTooManyRequestsError("429")
                for _ in range(len(_RETRY_DELAYS) - 1)
            ]
            + [None]
        )

        retry_on_429(fn)

        actual_delays = [c[0][0] for c in mock_sleep.call_args_list]
        assert actual_delays == _RETRY_DELAYS[: len(_RETRY_DELAYS) - 1]

    @patch("source_garmin.utils.time.sleep")
    def test_non_429_exception_propagates_immediately(self, mock_sleep):
        """Exceptions other than GarminConnectTooManyRequestsError are not retried."""
        fn = MagicMock(side_effect=ValueError("unexpected API shape"))

        with pytest.raises(ValueError, match="unexpected API shape"):
            retry_on_429(fn)

        fn.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("source_garmin.utils.time.sleep")
    def test_auth_error_propagates_immediately(self, mock_sleep):
        """GarminConnectAuthenticationError is not retried (wrong password won't fix itself)."""
        fn = MagicMock(
            side_effect=garminconnect.GarminConnectAuthenticationError("bad credentials")
        )

        with pytest.raises(garminconnect.GarminConnectAuthenticationError):
            retry_on_429(fn)

        fn.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("source_garmin.utils.time.sleep")
    def test_custom_delays_override_default(self, mock_sleep):
        """Custom delays list controls both the retry count and sleep durations."""
        fn = MagicMock(
            side_effect=[
                garminconnect.GarminConnectTooManyRequestsError("429"),
                "ok",
            ]
        )
        custom_delays = [5, 10]

        result = retry_on_429(fn, delays=custom_delays)

        assert result == "ok"
        mock_sleep.assert_called_once_with(5)
