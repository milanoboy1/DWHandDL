"""
Silver Layer Transformer – AWS Lambda Handler
===============================================
Reads raw JSON files from the bronze S3 bucket, applies cleaning and
flattening transformations, and writes structured data to the silver bucket.

Triggered by S3 PUT events on the bronze bucket (one invocation per new file).

Bronze S3 key pattern:
    raw/{source}/city={city}/date={YYYY-MM-DD}/{source}_{timestamp}.json

Silver S3 key pattern:
    silver/{source}/city={city}/date={YYYY-MM-DD}/{source}_{timestamp}.json

Environment variables:
    SILVER_BUCKET  – target S3 bucket for transformed data
    BRONZE_BUCKET  – source S3 bucket (for validation)
"""

import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

from transformers.flights import transform_flights
from transformers.weather import transform_weather

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "airoinsights-silver-588863")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "airoinsights-bronze-588863")

# Maps the "source" field in the bronze JSON to its transformer function.
# The ingestor Lambda uses these exact source names.
TRANSFORMERS = {
    "airlabs_flights":           transform_flights,
    "airlabs_flights_mock":      transform_flights,
    "open_meteo_weather":        transform_weather,
    "open_meteo_weather_mock":   transform_weather,
}


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def read_bronze_object(bucket: str, key: str) -> dict:
    """Download and parse a JSON object from the bronze bucket."""
    logger.info("Reading bronze object: s3://%s/%s", bucket, key)
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def write_silver_object(bucket: str, key: str, records: list[dict]) -> None:
    """
    Write transformed records to the silver bucket in JSON Lines format
    (one JSON object per line).  Athena's JsonSerDe reads each line as a
    separate row, so no UNNEST / CROSS JOIN is needed on the silver layer.
    """
    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    body = "\n".join(lines) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Written %d records to s3://%s/%s", len(records), bucket, key)


def build_silver_key(bronze_key: str) -> str:
    """
    Convert a bronze S3 key into a silver S3 key.

    Bronze: raw/flights/city=zurich/date=2026-05-14/flights_2026-05-14T12-00-00Z.json
    Silver: silver/flights/city=zurich/date=2026-05-14/flights_2026-05-14T12-00-00Z.json

    Simply replaces the 'raw/' prefix with 'silver/'.
    """
    if bronze_key.startswith("raw/"):
        return "silver/" + bronze_key[len("raw/"):]

    # Fallback: build a new key from scratch
    now = datetime.now(timezone.utc)
    return f"silver/unknown/date={now:%Y-%m-%d}/transformed_{now:%Y-%m-%dT%H-%M-%SZ}.json"


def detect_source(bronze_key: str, data: dict) -> str | None:
    """
    Determine the data source from the bronze object.
    Checks the 'source' field in the JSON first, then falls back to
    inspecting the S3 key path.
    """
    source = data.get("source")
    if source and source in TRANSFORMERS:
        return source

    # Fallback: check the key path  (e.g. raw/flights/... or raw/weather/...)
    key_lower = bronze_key.lower()
    if "/flights/" in key_lower:
        return "airlabs_flights"
    if "/weather/" in key_lower:
        return "open_meteo_weather"

    return None


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Triggered by S3 event notifications on the bronze bucket.

    Each record in the event corresponds to one new .json file in bronze.
    The handler reads the file, detects the data source, runs the matching
    transformer, and writes the result to the silver bucket.
    """
    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        # S3 event notifications URL-encode the key
        key = unquote_plus(record["s3"]["object"]["key"])

        # Skip non-JSON or objects outside raw/
        if not key.endswith(".json") or not key.startswith("raw/"):
            logger.warning("Skipping object: %s", key)
            continue

        try:
            # 1. Read bronze object
            bronze_data = read_bronze_object(bucket, key)

            # 2. Detect data source
            source = detect_source(key, bronze_data)
            if source is None:
                logger.warning("Unknown source for key %s – skipping", key)
                results.append({"key": key, "status": "skipped", "reason": "unknown_source"})
                continue

            # 3. Transform
            transformer = TRANSFORMERS[source]
            metadata = bronze_data.get("metadata", {})
            payload = bronze_data.get("payload", {})
            ingested_at = bronze_data.get("ingested_at_utc", "")

            silver_records = transformer(payload, metadata, ingested_at)

            if not silver_records:
                logger.info("No records produced for %s – skipping write", key)
                results.append({"key": key, "status": "skipped", "reason": "no_records"})
                continue

            # 4. Write to silver bucket (JSON Lines — one record per line)
            silver_key = build_silver_key(key)
            write_silver_object(SILVER_BUCKET, silver_key, silver_records)

            results.append({
                "key": key,
                "status": "success",
                "silver_key": silver_key,
                "records": len(silver_records),
            })

        except Exception as e:
            logger.error("Error processing %s: %s", key, str(e), exc_info=True)
            results.append({"key": key, "status": "error", "error": str(e)})

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": results}),
    }
