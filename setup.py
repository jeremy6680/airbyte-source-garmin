"""
Package setup for the Airbyte source connector for Garmin Connect.

This file tells Python (and pip) how to install the connector as a proper package.
When Airbyte runs the connector inside Docker it calls `pip install -e .` so that
`source_garmin` is importable from anywhere in the container.
"""

from setuptools import find_packages, setup

setup(
    name="airbyte-source-garmin",
    version="0.1.0",
    description="Airbyte source connector for Garmin Connect",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Jeremy Marchandeau",
    author_email="jerem9911@hotmail.com",
    url="https://github.com/jeremy6680/airbyte-source-garmin",
    license="MIT",
    python_requires=">=3.11",
    # find_packages() scans for every directory that contains an __init__.py
    # and treats it as an installable Python package.
    # We exclude test directories so they are not shipped in the final package.
    packages=find_packages(exclude=["unit_tests*", "integration_tests*"]),
    install_requires=[
        # Garmin Connect unofficial API client (SSO + garth session management)
        "garminconnect>=0.2.19",
        # Data transformation — the only allowed dataframe library per CLAUDE.md
        "pandas>=2.0.0",
        # Structured logging with coloured output and easy JSON sink support
        "loguru>=0.7.0",
        # Pydantic v2-based settings that reads from env vars and .env files
        "pydantic-settings>=2.1.0",
    ],
    # The console_scripts entry point lets Airbyte call `source-garmin` as a
    # command-line binary after `pip install -e .` — same as `python main.py`.
    entry_points={
        "console_scripts": [
            "source-garmin=main:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
