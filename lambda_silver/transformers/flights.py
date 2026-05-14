"""
Flights Transformer  (bronze → silver)
=======================================
Flattens raw AirLabs flight data into clean, queryable records.

Bronze structure:
    source:   "airlabs_flights"
    metadata: {"city": "zurich", "flight_count": 5, "source_iata": "ZRH", "is_mock": false}
    payload:  [   <-- flat list of flight dicts (already extracted from AirLabs "response")
        {
            "flight_iata": "LX316",
            "dep_iata": "ZRH",
            "arr_iata": "LHR",
            "status": "scheduled",
            "dep_time": "2026-05-14 08:00",
            "arr_time": "2026-05-14 09:10",
            "dep_actual": null,
            "arr_actual": null,
            "dep_delayed": 0,
            "arr_delayed": 0,
            "airline_iata": "LX",
            "airline_icao": "SWR",
            "aircraft_icao": "A320",
            "duration": 70,
            "delayed": 0
        },
        ...
    ]

Silver output: one flat dict per flight with cleaned fields and metadata.
"""

import logging

logger = logging.getLogger(__name__)


# Fields we extract from each raw flight record and rename for clarity
FIELD_MAP = {
    "flight_iata":     "flight_iata",
    "dep_iata":        "departure_iata",
    "arr_iata":        "arrival_iata",
    "airline_iata":    "airline_iata",
    "airline_icao":    "airline_icao",
    "aircraft_icao":   "aircraft_type",
    "status":          "flight_status",
    "dep_time":        "departure_time",
    "arr_time":        "arrival_time",
    "dep_actual":      "departure_actual",
    "arr_actual":      "arrival_actual",
    "dep_delayed":     "departure_delay_min",
    "arr_delayed":     "arrival_delay_min",
    "duration":        "duration_minutes",
    "delayed":         "delayed_flag",
}


def _clean_value(value):
    """Replace None and empty strings with None for consistency."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _safe_int(value) -> int | None:
    """Convert to int, return None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _normalize_status(status: str | None) -> str:
    """Normalize flight status to a consistent set of values."""
    if status is None:
        return "unknown"
    status = status.strip().lower()
    mapping = {
        "scheduled": "scheduled",
        "active":    "active",
        "en-route":  "active",
        "landed":    "landed",
        "arrived":   "landed",
        "cancelled": "cancelled",
        "canceled":  "cancelled",
        "diverted":  "diverted",
        "delayed":   "delayed",
    }
    return mapping.get(status, status)


def transform_flights(payload, metadata: dict, ingested_at: str) -> list[dict]:
    """
    Transform raw AirLabs flight data into clean silver records.

    Args:
        payload:     The 'payload' from the bronze JSON — a list of flight dicts.
        metadata:    The 'metadata' from the bronze JSON.
        ingested_at: The 'ingested_at_utc' timestamp from the bronze JSON.

    Returns:
        A list of flat, cleaned flight record dicts.
    """
    # The payload is already a flat list of flights
    # (the ingestor handler extracts them from AirLabs "response" before saving)
    if isinstance(payload, list):
        raw_flights = payload
    elif isinstance(payload, dict):
        # Fallback: if somehow wrapped in a dict with "response" key
        raw_flights = payload.get("response", [])
    else:
        logger.warning("Unexpected payload type: %s", type(payload))
        return []

    if not raw_flights:
        logger.warning("No flight records found in payload")
        return []

    silver_records = []
    seen_flights = set()  # Dedup by flight_iata + departure_time

    for raw in raw_flights:
        if not isinstance(raw, dict):
            continue

        # Build clean record
        record = {}
        for raw_field, silver_field in FIELD_MAP.items():
            record[silver_field] = _clean_value(raw.get(raw_field))

        # Normalize status
        record["flight_status"] = _normalize_status(record.get("flight_status"))

        # Ensure numeric fields are ints
        record["duration_minutes"] = _safe_int(record.get("duration_minutes"))
        record["departure_delay_min"] = _safe_int(record.get("departure_delay_min"))
        record["arrival_delay_min"] = _safe_int(record.get("arrival_delay_min"))
        record["delayed_flag"] = _safe_int(record.get("delayed_flag"))

        # Add metadata context
        record["city"] = metadata.get("city")
        record["source_iata"] = metadata.get("source_iata")
        record["is_mock"] = metadata.get("is_mock", False)
        record["ingested_at_utc"] = ingested_at

        # Deduplication
        dedup_key = (record.get("flight_iata"), record.get("departure_time"))
        if dedup_key in seen_flights:
            continue
        seen_flights.add(dedup_key)

        silver_records.append(record)

    logger.info(
        "Transformed %d flight records (from %d raw) for city=%s",
        len(silver_records), len(raw_flights), metadata.get("city", "?"),
    )
    return silver_records
