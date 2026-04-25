FROM python:3.11-slim

WORKDIR /airbyte/integration_code

# Install dependencies first so Docker can cache this layer independently of
# source code changes — a code edit won't re-run pip install unless
# requirements.txt also changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and entrypoint after dependencies to keep layers ordered
# from least-to-most frequently changing.
COPY source_garmin ./source_garmin
COPY main.py .
COPY setup.py .

# Airbyte uses this env var to discover the connector entrypoint when running
# inside the Airbyte platform. The Docker ENTRYPOINT below serves the same
# purpose for direct `docker run` invocations.
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"

ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
