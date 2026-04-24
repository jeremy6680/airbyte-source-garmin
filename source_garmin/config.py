"""
Connector configuration schema for airbyte-source-garmin.

This module defines ConnectorConfig, a Pydantic BaseSettings model that:
  - Validates the JSON file passed via `python main.py ... --config /secrets/config.json`
  - Provides typed, coerced fields (no raw dict access anywhere else in the codebase)
  - Generates the Airbyte SPEC JSON Schema automatically via .model_json_schema()
  - Masks the password in logs via SecretStr (access the raw value with .get_secret_value())
"""

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConnectorConfig(BaseSettings):
    """Validated configuration for the Garmin Connect source connector."""

    model_config = SettingsConfigDict(
        # Allow loading from environment variables prefixed with GARMIN_.
        # e.g. GARMIN_EMAIL, GARMIN_PASSWORD — useful for Docker deployments.
        env_prefix="GARMIN_",
        # Do not read a .env file automatically — credentials come from the
        # Airbyte-managed config file only.
        env_file=None,
    )

    email: str = Field(
        ...,
        description="Garmin Connect account email address.",
        json_schema_extra={"order": 0},
    )

    # SecretStr prevents the password from appearing in repr() or log output.
    # To pass it to garminconnect.Garmin(), call config.password.get_secret_value().
    password: SecretStr = Field(
        ...,
        description="Garmin Connect account password.",
        # airbyte_secret instructs the Airbyte UI to mask this field.
        json_schema_extra={"airbyte_secret": True, "order": 1},
    )

    lookback_days: int = Field(
        default=30,
        ge=1,
        le=365,
        description=(
            "Number of calendar days to look back when performing a full-refresh sync. "
            "Minimum 1, maximum 365."
        ),
        json_schema_extra={"order": 2},
    )

    session_file_path: str = Field(
        default="/tmp/garmin_session.json",
        description=(
            "Path to the file where the Garmin OAuth session token is cached between "
            "runs. Use a Docker volume mount to persist this across container restarts."
        ),
        json_schema_extra={"order": 3},
    )

    @field_validator("email")
    @classmethod
    def email_must_not_be_empty(cls, value: str) -> str:
        """Reject blank email strings that pass the `str` type check.

        Args:
            value: The raw email string from config.

        Returns:
            The stripped email string if non-empty.

        Raises:
            ValueError: If the email is blank after stripping whitespace.
        """
        if not value.strip():
            raise ValueError("email must not be empty")
        return value.strip()

    @field_validator("password", mode="before")
    @classmethod
    def password_must_not_be_empty(cls, value: str) -> str:
        """Reject blank password strings before SecretStr wrapping occurs.

        mode="before" means this validator receives the raw string from the JSON
        file, before Pydantic wraps it in SecretStr. Returning a plain str here
        is correct — Pydantic will wrap it afterwards.

        Args:
            value: The raw password string from config.

        Returns:
            The password string unchanged if non-empty.

        Raises:
            ValueError: If the password is blank after stripping whitespace.
        """
        if not str(value).strip():
            raise ValueError("password must not be empty")
        return value


def load_config(config_path: str) -> ConnectorConfig:
    """Parse and validate a connector config JSON file.

    Reads the JSON file at config_path and instantiates ConnectorConfig,
    letting Pydantic raise a clear ValidationError for any missing or
    invalid field before the connector does any real work.

    Args:
        config_path: Absolute or relative path to the Airbyte config JSON file.

    Returns:
        A fully validated ConnectorConfig instance.

    Raises:
        FileNotFoundError: If config_path does not exist.
        pydantic.ValidationError: If a required field is missing or invalid.
    """
    import json

    with open(config_path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)

    return ConnectorConfig(**raw)


def build_spec() -> dict:
    """Generate the Airbyte SPEC payload from the ConnectorConfig JSON Schema.

    Derives the connection specification directly from the Pydantic model so
    there is a single source of truth — no hand-written JSON Schema to maintain.

    The class-level docstring is stripped from the schema (KB-005) to avoid
    leaking Python implementation details into the Airbyte UI description field.

    Returns:
        A dict shaped as an Airbyte AirbyteMessage SPEC payload, ready to be
        serialised to JSON and printed to stdout.
    """
    schema = ConnectorConfig.model_json_schema()

    # Remove the class docstring that Pydantic injects as "description".
    # Field-level descriptions (email, password, etc.) are kept — they are useful.
    schema.pop("description", None)

    return {
        "type": "SPEC",
        "spec": {
            "documentationUrl": "https://github.com/jeremy6680/airbyte-source-garmin",
            "connectionSpecification": schema,
        },
    }
