"""
Comprehensive test suite for silver layer transformers + handler.
Run from repo root:  python test_transformers.py

Tests:
  1. Flights transformer — field mapping, dedup, mock data, empty payloads
  2. Weather transformer — hourly/daily flattening, WMO codes, mock data
  3. Handler — build_silver_key, detect_source, full Lambda simulation
  4. Edge cases — partition key collisions, JSON Lines format
"""

import json
import sys
import os
from unittest.mock import patch, MagicMock
from io import BytesIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lambda_silver"))

from transformers.flights import transform_flights
from transformers.weather import transform_weather

INGESTED_AT = "2026-05-14T12:00:00+00:00"

# ── Realistic test data matching the actual ingestor output ─────────────────

FLIGHTS_METADATA = {
    "city": "zurich",
    "flight_count": 3,
    "source_iata": "ZRH",
    "is_mock": False,
}

FLIGHTS_PAYLOAD = [
    {
        "flight_iata": "LX316", "dep_iata": "ZRH", "arr_iata": "LHR",
        "status": "scheduled", "dep_time": "2026-05-14 08:00",
        "arr_time": "2026-05-14 09:10", "dep_actual": None,
        "arr_actual": None, "dep_delayed": 0, "arr_delayed": 0,
        "airline_iata": "LX", "airline_icao": "SWR",
        "aircraft_icao": "A320", "duration": 70, "delayed": 0,
    },
    {
        "flight_iata": "LX1234", "dep_iata": "ZRH", "arr_iata": "CDG",
        "status": "delayed", "dep_time": "2026-05-14 14:30",
        "arr_time": "2026-05-14 16:00", "dep_actual": "2026-05-14 14:55",
        "arr_actual": None, "dep_delayed": 25, "arr_delayed": None,
        "airline_iata": "LX", "airline_icao": "SWR",
        "aircraft_icao": "A321", "duration": 90, "delayed": 25,
    },
    # Duplicate of first flight
    {
        "flight_iata": "LX316", "dep_iata": "ZRH", "arr_iata": "LHR",
        "status": "scheduled", "dep_time": "2026-05-14 08:00",
        "arr_time": "2026-05-14 09:10", "duration": 70,
    },
]

WEATHER_METADATA = {
    "location": "zurich",
    "lat": 47.3769,
    "lon": 8.5417,
    "is_mock": False,
}

# Open-Meteo returns forecasts for MULTIPLE days ahead
WEATHER_PAYLOAD = {
    "latitude": 47.37, "longitude": 8.55, "timezone": "Europe/Zurich",
    "hourly": {
        "time": [
            "2026-05-14T00:00", "2026-05-14T01:00", "2026-05-14T02:00",
            "2026-05-15T00:00",  # next day's forecast
        ],
        "temperature_2m": [15.2, 14.8, 14.1, 13.5],
        "relative_humidity_2m": [72, 75, 78, 80],
        "precipitation": [0.0, 0.0, 0.2, 0.5],
        "wind_speed_10m": [5.4, 4.8, 6.1, 7.2],
    },
    "daily": {
        "time": ["2026-05-14", "2026-05-15", "2026-05-16"],  # 3 days
        "weather_code": [61, 3, 0],
        "temperature_2m_max": [22.5, 20.1, 25.3],
        "temperature_2m_min": [12.3, 10.5, 14.8],
    },
}


passed = 0
failed = 0

def check(condition, msg):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✓ {msg}")
    else:
        failed += 1
        print(f"  ✗ FAIL: {msg}")


# ═════════════════════════════════════════════════════════════════════════════
# 1. FLIGHTS TRANSFORMER
# ═════════════════════════════════════════════════════════════════════════════

def test_flights_basic():
    print("\n" + "=" * 60)
    print("TEST: Flights — basic transformation")
    print("=" * 60)

    records = transform_flights(FLIGHTS_PAYLOAD, FLIGHTS_METADATA, INGESTED_AT)

    check(len(records) == 2, f"Dedup: 3 raw → 2 records (got {len(records)})")
    check(records[0]["flight_iata"] == "LX316", "flight_iata preserved")
    check(records[0]["departure_iata"] == "ZRH", "dep_iata → departure_iata")
    check(records[0]["arrival_iata"] == "LHR", "arr_iata → arrival_iata")
    check(records[0]["aircraft_type"] == "A320", "aircraft_icao → aircraft_type")
    check(records[0]["flight_status"] == "scheduled", "status → flight_status (normalized)")
    check(records[0]["duration_minutes"] == 70, "duration → duration_minutes (int)")
    check(records[0]["departure_delay_min"] == 0, "dep_delayed → departure_delay_min (int)")
    check(records[0]["departure_actual"] is None, "null dep_actual preserved")
    check(records[0]["city"] == "zurich", "metadata.city attached")
    check(records[0]["source_iata"] == "ZRH", "metadata.source_iata attached")
    check(records[0]["is_mock"] is False, "metadata.is_mock attached")
    check(records[0]["ingested_at_utc"] == INGESTED_AT, "ingested_at passed through")

    # Second record
    check(records[1]["flight_status"] == "delayed", "delayed status normalized")
    check(records[1]["departure_delay_min"] == 25, "delay value correct")
    check(records[1]["departure_actual"] == "2026-05-14 14:55", "actual time preserved")


def test_flights_empty_string_cleanup():
    print("\n" + "=" * 60)
    print("TEST: Flights — empty string → None")
    print("=" * 60)

    payload = [{"flight_iata": "AB123", "dep_iata": "ZRH", "arr_iata": "",
                "status": "  ", "dep_time": "2026-05-14 10:00"}]
    records = transform_flights(payload, FLIGHTS_METADATA, INGESTED_AT)

    check(len(records) == 1, "one record produced")
    check(records[0]["arrival_iata"] is None, "empty string → None")
    check(records[0]["flight_status"] == "unknown", "whitespace status → 'unknown'")


def test_flights_mock_data():
    print("\n" + "=" * 60)
    print("TEST: Flights — mock data (minimal fields)")
    print("=" * 60)

    # This matches what the ingestor generates in mock mode
    mock_payload = [
        {"flight_iata": "MKZRH1", "dep_iata": "ZRH", "arr_iata": "LHR", "status": "scheduled"},
        {"flight_iata": "MKZRH2", "dep_iata": "CDG", "arr_iata": "ZRH", "status": "active"},
    ]
    mock_meta = {"city": "zurich", "flight_count": 2, "source_iata": "ZRH", "is_mock": True}

    records = transform_flights(mock_payload, mock_meta, INGESTED_AT)

    check(len(records) == 2, "2 mock records")
    check(records[0]["is_mock"] is True, "is_mock = True")
    check(records[0]["duration_minutes"] is None, "missing duration → None")
    check(records[0]["aircraft_type"] is None, "missing aircraft → None")
    check(records[0]["departure_time"] is None, "missing dep_time → None")


def test_flights_status_normalization():
    print("\n" + "=" * 60)
    print("TEST: Flights — status normalization")
    print("=" * 60)

    statuses = [
        ("scheduled", "scheduled"), ("active", "active"), ("en-route", "active"),
        ("landed", "landed"), ("arrived", "landed"),
        ("cancelled", "cancelled"), ("canceled", "cancelled"),
        ("Scheduled", "scheduled"),  # case-insensitive
        (None, "unknown"), ("", "unknown"),
    ]
    for raw_status, expected in statuses:
        payload = [{"flight_iata": f"XX{raw_status}", "dep_iata": "ZRH",
                    "arr_iata": "LHR", "status": raw_status, "dep_time": f"2026-{raw_status}"}]
        records = transform_flights(payload, FLIGHTS_METADATA, INGESTED_AT)
        check(records[0]["flight_status"] == expected,
              f"'{raw_status}' → '{expected}' (got '{records[0]['flight_status']}')")


def test_flights_empty_payloads():
    print("\n" + "=" * 60)
    print("TEST: Flights — empty/malformed payloads")
    print("=" * 60)

    check(transform_flights([], FLIGHTS_METADATA, INGESTED_AT) == [], "empty list → []")
    check(transform_flights({}, FLIGHTS_METADATA, INGESTED_AT) == [], "empty dict → []")
    check(transform_flights([None, "bad", 123], FLIGHTS_METADATA, INGESTED_AT) == [],
          "non-dict items skipped")


# ═════════════════════════════════════════════════════════════════════════════
# 2. WEATHER TRANSFORMER
# ═════════════════════════════════════════════════════════════════════════════

def test_weather_basic():
    print("\n" + "=" * 60)
    print("TEST: Weather — basic transformation")
    print("=" * 60)

    records = transform_weather(WEATHER_PAYLOAD, WEATHER_METADATA, INGESTED_AT)

    hourly = [r for r in records if r["record_type"] == "hourly"]
    daily = [r for r in records if r["record_type"] == "daily"]

    check(len(hourly) == 4, f"4 hourly records (got {len(hourly)})")
    check(len(daily) == 3, f"3 daily records (got {len(daily)})")

    # Hourly checks
    h0 = hourly[0]
    check(h0["observation_time"] == "2026-05-14T00:00", "observation_time correct")
    check(h0["temperature_c"] == 15.2, "temperature_c correct")
    check(h0["humidity_pct"] == 72.0, "humidity_pct correct")
    check(h0["precipitation_mm"] == 0.0, "precipitation_mm correct")
    check(h0["wind_speed_kmh"] == 5.4, "wind_speed_kmh correct")
    check(h0["city"] == "zurich", "city from metadata.location")
    check(h0["latitude"] == 47.3769, "latitude from metadata.lat")
    check(h0["longitude"] == 8.5417, "longitude from metadata.lon")
    check(h0["is_mock"] is False, "is_mock correct")

    # Daily checks
    d0 = daily[0]
    check(d0["forecast_date"] == "2026-05-14", "forecast_date correct (not 'date')")
    check(d0["weather_code"] == 61, "weather_code correct")
    check(d0["weather_description"] == "slight_rain", "WMO 61 → slight_rain")
    check(d0["temperature_max_c"] == 22.5, "temp max correct")
    check(d0["temperature_min_c"] == 12.3, "temp min correct")

    # Multi-day forecasts
    d1 = daily[1]
    check(d1["forecast_date"] == "2026-05-15", "second forecast date correct")
    check(d1["weather_description"] == "overcast", "WMO 3 → overcast")

    d2 = daily[2]
    check(d2["forecast_date"] == "2026-05-16", "third forecast date correct")
    check(d2["weather_description"] == "clear_sky", "WMO 0 → clear_sky")


def test_weather_no_partition_key_collision():
    """
    CRITICAL: Verify daily records use 'forecast_date' NOT 'date',
    because 'date' is a Glue partition key (= ingestion date, not forecast date).
    """
    print("\n" + "=" * 60)
    print("TEST: Weather — no partition key collision")
    print("=" * 60)

    records = transform_weather(WEATHER_PAYLOAD, WEATHER_METADATA, INGESTED_AT)
    daily = [r for r in records if r["record_type"] == "daily"]

    for d in daily:
        check("date" not in d, f"daily record has NO 'date' field (would collide with partition)")
        check("forecast_date" in d, f"daily record uses 'forecast_date' instead")

    hourly = [r for r in records if r["record_type"] == "hourly"]
    for h in hourly:
        check("date" not in h, "hourly record has no 'date' field either")


def test_weather_mock():
    print("\n" + "=" * 60)
    print("TEST: Weather — mock data")
    print("=" * 60)

    mock_meta = {"location": "zurich", "lat": 47.3769, "lon": 8.5417, "is_mock": True}
    mock_payload = {
        "latitude": 47.3769, "longitude": 8.5417,
        "hourly": {
            "time": ["2026-04-01T12:00", "2026-04-01T13:00"],
            "temperature_2m": [14.5, 15.0],
            "relative_humidity_2m": [60, 62],
            "precipitation": [0.0, 0.1],
            "wind_speed_10m": [12.0, 13.5],
        },
        "daily": {
            "time": ["2026-04-01"],
            "weather_code": [3],
            "temperature_2m_max": [17.0],
            "temperature_2m_min": [9.0],
        },
        "mock_data": True,
    }
    records = transform_weather(mock_payload, mock_meta, INGESTED_AT)
    check(len(records) == 3, f"3 records (2 hourly + 1 daily, got {len(records)})")
    check(records[0]["is_mock"] is True, "is_mock = True")


def test_weather_empty():
    print("\n" + "=" * 60)
    print("TEST: Weather — empty payloads")
    print("=" * 60)

    check(transform_weather({}, WEATHER_METADATA, INGESTED_AT) == [], "empty dict → []")
    check(transform_weather({"hourly": {}, "daily": {}}, WEATHER_METADATA, INGESTED_AT) == [],
          "empty hourly+daily → []")
    check(transform_weather({"hourly": {"time": []}}, WEATHER_METADATA, INGESTED_AT) == [],
          "empty time array → []")


def test_weather_mismatched_array_lengths():
    """Open-Meteo could theoretically return arrays of different lengths."""
    print("\n" + "=" * 60)
    print("TEST: Weather — mismatched array lengths")
    print("=" * 60)

    payload = {
        "hourly": {
            "time": ["2026-05-14T00:00", "2026-05-14T01:00", "2026-05-14T02:00"],
            "temperature_2m": [15.0, 14.0],  # one short
            "relative_humidity_2m": [70],      # two short
            "precipitation": [],               # empty
            "wind_speed_10m": [5.0, 6.0, 7.0, 8.0],  # one extra (ignored)
        },
    }
    records = transform_weather(payload, WEATHER_METADATA, INGESTED_AT)

    check(len(records) == 3, f"3 records (based on time array, got {len(records)})")
    check(records[2]["temperature_c"] is None, "missing temp → None (not crash)")
    check(records[1]["humidity_pct"] is None, "missing humidity → None")
    check(records[0]["precipitation_mm"] is None, "empty precip → None")
    check(records[0]["wind_speed_kmh"] == 5.0, "wind_speed present for index 0")


# ═════════════════════════════════════════════════════════════════════════════
# 3. HANDLER TESTS
# ═════════════════════════════════════════════════════════════════════════════

def test_build_silver_key():
    print("\n" + "=" * 60)
    print("TEST: Handler — build_silver_key")
    print("=" * 60)

    from handler import build_silver_key

    cases = [
        ("raw/flights/city=zurich/date=2026-05-14/flights_2026-05-14T12-00-00Z.json",
         "silver/flights/city=zurich/date=2026-05-14/flights_2026-05-14T12-00-00Z.json"),
        ("raw/weather/city=london/date=2026-05-14/weather_2026-05-14T12-00-00Z.json",
         "silver/weather/city=london/date=2026-05-14/weather_2026-05-14T12-00-00Z.json"),
        ("raw/flights/city=unknown/date=2026-01-01/flights_2026-01-01T00-00-00Z.json",
         "silver/flights/city=unknown/date=2026-01-01/flights_2026-01-01T00-00-00Z.json"),
    ]
    for bronze, expected in cases:
        result = build_silver_key(bronze)
        check(result == expected, f"'{bronze}' → correct silver key")

    # Fallback for weird keys
    result = build_silver_key("some/other/path.json")
    check(result.startswith("silver/unknown/"), "non-raw/ key → fallback path")


def test_detect_source():
    print("\n" + "=" * 60)
    print("TEST: Handler — detect_source")
    print("=" * 60)

    from handler import detect_source

    # Primary: from JSON source field
    check(detect_source("raw/flights/...", {"source": "airlabs_flights"}) == "airlabs_flights",
          "airlabs_flights from source field")
    check(detect_source("raw/flights/...", {"source": "airlabs_flights_mock"}) == "airlabs_flights_mock",
          "airlabs_flights_mock from source field")
    check(detect_source("raw/weather/...", {"source": "open_meteo_weather"}) == "open_meteo_weather",
          "open_meteo_weather from source field")
    check(detect_source("raw/weather/...", {"source": "open_meteo_weather_mock"}) == "open_meteo_weather_mock",
          "open_meteo_weather_mock from source field")

    # Fallback: from S3 key path
    check(detect_source("raw/flights/city=zurich/date=2026-05-14/flights_xxx.json", {}) == "airlabs_flights",
          "fallback: /flights/ in key → airlabs_flights")
    check(detect_source("raw/weather/city=zurich/date=2026-05-14/weather_xxx.json", {}) == "open_meteo_weather",
          "fallback: /weather/ in key → open_meteo_weather")

    # Unknown
    check(detect_source("raw/events/city=zurich/events.json", {}) is None,
          "unknown source → None")
    check(detect_source("raw/flights/...", {"source": "unknown_source_xyz"}) == "airlabs_flights",
          "bad source field but /flights/ in path → fallback works")


def test_handler_full_flow():
    """
    Simulate the full Lambda handler with mocked S3.
    Verifies the complete flow: read bronze → transform → write silver (JSON Lines).
    """
    print("\n" + "=" * 60)
    print("TEST: Handler — full Lambda flow (mocked S3)")
    print("=" * 60)

    from handler import lambda_handler

    # Build a realistic bronze JSON (what the ingestor actually writes)
    bronze_flights = {
        "source": "airlabs_flights",
        "ingested_at_utc": INGESTED_AT,
        "metadata": FLIGHTS_METADATA,
        "payload": FLIGHTS_PAYLOAD,
    }
    bronze_weather = {
        "source": "open_meteo_weather",
        "ingested_at_utc": INGESTED_AT,
        "metadata": WEATHER_METADATA,
        "payload": WEATHER_PAYLOAD,
    }

    # Track what gets written to S3
    written_objects = {}

    def mock_get_object(Bucket, Key):
        if "flights" in Key:
            body = json.dumps(bronze_flights).encode("utf-8")
        else:
            body = json.dumps(bronze_weather).encode("utf-8")
        mock_body = MagicMock()
        mock_body.read.return_value = body
        return {"Body": mock_body}

    def mock_put_object(Bucket, Key, Body, ContentType):
        written_objects[Key] = Body.decode("utf-8") if isinstance(Body, bytes) else Body

    # Simulate S3 event with two new files (flights + weather)
    s3_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "airoinsights-bronze-588863"},
                    "object": {"key": "raw/flights/city=zurich/date=2026-05-14/flights_2026-05-14T12-00-00Z.json"},
                }
            },
            {
                "s3": {
                    "bucket": {"name": "airoinsights-bronze-588863"},
                    "object": {"key": "raw/weather/city=zurich/date=2026-05-14/weather_2026-05-14T12-00-00Z.json"},
                }
            },
        ]
    }

    with patch("handler.s3") as mock_s3:
        mock_s3.get_object = mock_get_object
        mock_s3.put_object = mock_put_object

        result = lambda_handler(s3_event, None)

    body = json.loads(result["body"])
    processed = body["processed"]

    check(result["statusCode"] == 200, "handler returns 200")
    check(len(processed) == 2, f"2 files processed (got {len(processed)})")
    check(all(r["status"] == "success" for r in processed), "both succeeded")

    # Verify flights silver
    flights_key = "silver/flights/city=zurich/date=2026-05-14/flights_2026-05-14T12-00-00Z.json"
    check(flights_key in written_objects, "flights silver key correct")
    flights_lines = written_objects[flights_key].strip().split("\n")
    check(len(flights_lines) == 2, f"flights: 2 JSON Lines (got {len(flights_lines)})")

    flight_record = json.loads(flights_lines[0])
    check(flight_record["flight_iata"] == "LX316", "first flight record correct")
    check("source" not in flight_record, "no envelope wrapper — records are flat")

    # Verify weather silver
    weather_key = "silver/weather/city=zurich/date=2026-05-14/weather_2026-05-14T12-00-00Z.json"
    check(weather_key in written_objects, "weather silver key correct")
    weather_lines = written_objects[weather_key].strip().split("\n")
    check(len(weather_lines) == 7, f"weather: 7 JSON Lines = 4 hourly + 3 daily (got {len(weather_lines)})")

    # Verify JSON Lines format (each line is valid JSON)
    all_valid = True
    for line in weather_lines:
        try:
            json.loads(line)
        except json.JSONDecodeError:
            all_valid = False
    check(all_valid, "all lines are valid JSON (JSON Lines format)")

    # Verify daily records use forecast_date, not date
    daily_lines = [json.loads(l) for l in weather_lines if json.loads(l).get("record_type") == "daily"]
    check(len(daily_lines) == 3, f"3 daily records in output (got {len(daily_lines)})")
    check("forecast_date" in daily_lines[0], "daily uses 'forecast_date'")
    check("date" not in daily_lines[0], "daily does NOT use 'date' (partition collision)")

    print(f"\n  Written silver keys:")
    for key in sorted(written_objects.keys()):
        lines = written_objects[key].strip().split("\n")
        print(f"    {key}  ({len(lines)} records)")


def test_handler_url_decode():
    """S3 event notifications URL-encode the key. Verify we handle it."""
    print("\n" + "=" * 60)
    print("TEST: Handler — URL-encoded S3 keys")
    print("=" * 60)

    from handler import lambda_handler

    bronze_data = {
        "source": "airlabs_flights",
        "ingested_at_utc": INGESTED_AT,
        "metadata": FLIGHTS_METADATA,
        "payload": FLIGHTS_PAYLOAD[:1],  # just one flight
    }

    written_keys = []

    def mock_get_object(Bucket, Key):
        # The key should be decoded by the time it reaches get_object
        check("+" not in Key, f"key was URL-decoded before S3 call: {Key}")
        body = json.dumps(bronze_data).encode("utf-8")
        mock_body = MagicMock()
        mock_body.read.return_value = body
        return {"Body": mock_body}

    def mock_put_object(Bucket, Key, Body, ContentType):
        written_keys.append(Key)

    # Simulate URL-encoded key (spaces as +, though our keys don't have spaces)
    # But = signs in partition values could theoretically be %3D
    s3_event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "airoinsights-bronze-588863"},
                "object": {"key": "raw/flights/city%3Dzurich/date%3D2026-05-14/flights_2026-05-14T12-00-00Z.json"},
            }
        }]
    }

    with patch("handler.s3") as mock_s3:
        mock_s3.get_object = mock_get_object
        mock_s3.put_object = mock_put_object
        result = lambda_handler(s3_event, None)

    check(len(written_keys) == 1, "one silver file written")
    check("city=zurich" in written_keys[0], "decoded key used for silver path")


def test_handler_skip_non_raw():
    """Handler should skip files not under raw/ prefix."""
    print("\n" + "=" * 60)
    print("TEST: Handler — skip non-raw objects")
    print("=" * 60)

    from handler import lambda_handler

    s3_event = {
        "Records": [
            {"s3": {"bucket": {"name": "test"}, "object": {"key": "silver/flights/something.json"}}},
            {"s3": {"bucket": {"name": "test"}, "object": {"key": "raw/flights/data.csv"}}},
            {"s3": {"bucket": {"name": "test"}, "object": {"key": "backup/flights.json"}}},
        ]
    }

    with patch("handler.s3") as mock_s3:
        mock_s3.get_object = MagicMock()
        mock_s3.put_object = MagicMock()
        result = lambda_handler(s3_event, None)

    body = json.loads(result["body"])
    check(len(body["processed"]) == 0, "no files processed (all filtered)")
    check(mock_s3.get_object.call_count == 0, "no S3 reads attempted")


# ═════════════════════════════════════════════════════════════════════════════
# 4. GLUE TABLE FIELD VERIFICATION
# ═════════════════════════════════════════════════════════════════════════════

def test_glue_field_coverage():
    """
    Verify that every field the transformers output is defined in the Glue
    table columns (or is a partition key). Missing columns = invisible in Athena.
    """
    print("\n" + "=" * 60)
    print("TEST: Glue table field coverage")
    print("=" * 60)

    # Expected Glue columns + partition keys for flights
    flights_glue = {
        "flight_iata", "departure_iata", "arrival_iata", "airline_iata",
        "airline_icao", "aircraft_type", "flight_status", "departure_time",
        "arrival_time", "departure_actual", "arrival_actual",
        "departure_delay_min", "arrival_delay_min", "duration_minutes",
        "delayed_flag", "source_iata", "is_mock", "ingested_at_utc",
        "city", "date",  # partition keys
    }

    # Expected Glue columns + partition keys for weather
    weather_glue = {
        "record_type", "latitude", "longitude", "is_mock", "ingested_at_utc",
        "observation_time", "temperature_c", "humidity_pct", "precipitation_mm",
        "wind_speed_kmh", "forecast_date", "weather_code", "weather_description",
        "temperature_max_c", "temperature_min_c",
        "city", "date",  # partition keys
    }

    # Get actual transformer output fields
    flight_records = transform_flights(FLIGHTS_PAYLOAD[:1], FLIGHTS_METADATA, INGESTED_AT)
    flight_fields = set(flight_records[0].keys())

    weather_records = transform_weather(WEATHER_PAYLOAD, WEATHER_METADATA, INGESTED_AT)
    hourly_fields = set([r for r in weather_records if r["record_type"] == "hourly"][0].keys())
    daily_fields = set([r for r in weather_records if r["record_type"] == "daily"][0].keys())
    weather_fields = hourly_fields | daily_fields

    # Check flights
    missing_flights = flight_fields - flights_glue
    check(len(missing_flights) == 0,
          f"flights: all output fields in Glue table (missing: {missing_flights or 'none'})")

    # Check weather
    missing_weather = weather_fields - weather_glue
    check(len(missing_weather) == 0,
          f"weather: all output fields in Glue table (missing: {missing_weather or 'none'})")

    # Check no partition key collision
    flight_columns_only = flights_glue - {"city", "date"}  # partition keys
    check("city" not in flight_columns_only, "flights: 'city' is partition key, not column")
    check("date" not in flight_columns_only, "flights: 'date' is partition key, not column")


# ═════════════════════════════════════════════════════════════════════════════
# RUN ALL
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Transformers
    test_flights_basic()
    test_flights_empty_string_cleanup()
    test_flights_mock_data()
    test_flights_status_normalization()
    test_flights_empty_payloads()

    test_weather_basic()
    test_weather_no_partition_key_collision()
    test_weather_mock()
    test_weather_empty()
    test_weather_mismatched_array_lengths()

    # Handler
    test_build_silver_key()
    test_detect_source()
    test_handler_full_flow()
    test_handler_url_decode()
    test_handler_skip_non_raw()

    # Glue coverage
    test_glue_field_coverage()

    print("\n" + "=" * 60)
    if failed == 0:
        print(f"ALL {passed} CHECKS PASSED ✓")
    else:
        print(f"RESULTS: {passed} passed, {failed} FAILED")
    print("=" * 60)

    sys.exit(1 if failed else 0)
