"""
Shared utility functions for airbyte-source-garmin.

Provides retry_on_429 — a thin wrapper for Garmin API calls that automatically
retries on HTTP 429 (Too Many Requests) with exponential backoff.

This module is the single source of truth for retry behaviour.  Both the
authentication layer (GarminAuth) and the stream layer (read_records() in each
stream) delegate to retry_on_429 so the backoff schedule is defined and tested
in one place.
"""

import time
from typing import Any, Callable, List, Optional

import garminconnect
from loguru import logger

# Default retry schedule applied between consecutive HTTP 429 responses.
# Index 0 → wait after attempt 1, index 1 → wait after attempt 2, etc.
# len(_RETRY_DELAYS) equals the maximum number of total attempts.
_RETRY_DELAYS: List[int] = [30, 60, 120]


def retry_on_429(
    fn: Callable[[], Any],
    delays: Optional[List[int]] = None,
) -> Any:
    """Call fn(), retrying on HTTP 429 with increasing backoff delays.

    Designed to wrap any Garmin API call — both the initial login in GarminAuth
    and the per-record data fetches in stream read_records() methods.

    Only GarminConnectTooManyRequestsError triggers a retry.  All other
    exceptions propagate immediately so callers can handle them as appropriate
    (e.g. log-and-skip for missing daily summaries, fatal raise for activities).

    Args:
        fn: Zero-argument callable wrapping the Garmin API call.
            Use a lambda to forward arguments:
            ``lambda: client.get_activities_by_date(start, end)``.
        delays: Wait durations in seconds for each retry.  ``len(delays)`` is
            the total number of attempts.  Defaults to ``_RETRY_DELAYS``
            (30 s, 60 s, 120 s → 3 attempts).

    Returns:
        The return value of fn() on first success.

    Raises:
        garminconnect.GarminConnectTooManyRequestsError: When every attempt
            is rate-limited and retries are exhausted.
        Any other exception raised by fn() propagates without modification.
    """
    if delays is None:
        delays = _RETRY_DELAYS

    max_attempts = len(delays)

    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except garminconnect.GarminConnectTooManyRequestsError as exc:
            if attempt < max_attempts:
                delay = delays[attempt - 1]
                logger.warning(
                    "Garmin rate-limited (429) on attempt {}/{}. "
                    "Waiting {}s before next attempt.",
                    attempt,
                    max_attempts,
                    delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "Garmin API rate limit persisted after {} attempt(s).",
                    max_attempts,
                )
                raise
