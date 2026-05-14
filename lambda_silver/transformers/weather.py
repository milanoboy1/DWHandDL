"""
Weather Transformer  (bronze → silver)
=======================================
Flattens the raw Open-Meteo forecast response into one row per hour and
one row per day — ready for Athena queries.

Bronze structure:
    source:   "open_meteo_weather"
    metadata: {"location": "zurich", "lat": 47.37, "lon": 8.54, "is_mock": false}
    payload:  {
        "latitude": 47.37,
        "longitude": 8.55,
        "hourly": {
            "time":                  ["2026-05-14T00:00", "2026-05-14T01:00", ...],
            "temperature_2m":        [15.2, 14.8, ...],
            "relative_humidity_2m":  [72, 75, ...],
            "precipitation":         [0.0, 0.0, ...],
            "wind_speed_10m":        [5.4, 4.8, ...]
        },
        "daily": {
            "time":                  ["2026-05-14", ...],
            "weather_code":          [61, ...],
            "temperature_2m_max":    [22.5, ...],
            "temperature_2m_min":    [12.3, ...]
        }
    }

Silver output: flat dicts tagged with record_type "hourly" or "daily".
"""

import logging

logger = logging.getLogger(__name__)


# WMO weather codes → human-readable descriptions
WMO_CODES = {
    0:  "clear_sky",
    1:  "mainly_clear",
    2:  "partly_cloudy",
    3:  "overcast",
    45: "fog",
    48: "depositing_rime_fog",
    51: "light_drizzle",
    53: "moderate_drizzle",
    55: "dense_drizzle",
    61: "slight_rain",
    63: "moderate_rain",
    65: "heavy_rain",
    71: "slight_snowfall",
    73: "moderate_snowfall",
    75: "heavy_snowfall",
    80: "slight_rain_showers",
    81: "moderate_rain_showers",
    82: "violent_rain_showers",
    95: "thunderstorm",
    96: "thunderstorm_with_slight_hail",
    99: "thunderstorm_with_heavy_hail",
}


def _safe_float(value) -> float | None:
    """Convert to float, return None on failure."""
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (ValueError, TypeError):
        return None


def _safe_get(lst: list, idx: int):
    """Safely index into a list."""
    if idx < len(lst):
        return lst[idx]
    return None


def _flatten_hourly(hourly: dict, metadata: dict, ingested_at: str) -> list[dict]:
    """
    Convert column-oriented hourly arrays into one row per hour.
    """
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    humidities = hourly.get("relative_humidity_2m", [])
    precipitation = hourly.get("precipitation", [])
    wind_speed = hourly.get("wind_speed_10m", [])

    location = metadata.get("location")
    lat = metadata.get("lat")
    lon = metadata.get("lon")
    is_mock = metadata.get("is_mock", False)

    records = []
    for i in range(len(times)):
        records.append({
            "record_type":        "hourly",
            "observation_time":   times[i],
            "temperature_c":      _safe_float(_safe_get(temps, i)),
            "humidity_pct":       _safe_float(_safe_get(humidities, i)),
            "precipitation_mm":   _safe_float(_safe_get(precipitation, i)),
            "wind_speed_kmh":     _safe_float(_safe_get(wind_speed, i)),
            "city":               location,
            "latitude":           lat,
            "longitude":          lon,
            "is_mock":            is_mock,
            "ingested_at_utc":    ingested_at,
        })

    return records


def _flatten_daily(daily: dict, metadata: dict, ingested_at: str) -> list[dict]:
    """
    Convert column-oriented daily arrays into one row per day.
    """
    times = daily.get("time", [])
    weather_codes = daily.get("weather_code", [])
    temp_max = daily.get("temperature_2m_max", [])
    temp_min = daily.get("temperature_2m_min", [])

    location = metadata.get("location")
    lat = metadata.get("lat")
    lon = metadata.get("lon")
    is_mock = metadata.get("is_mock", False)

    records = []
    for i in range(len(times)):
        code = _safe_get(weather_codes, i)
        records.append({
            "record_type":           "daily",
            "forecast_date":         times[i],
            "weather_code":          code,
            "weather_description":   WMO_CODES.get(code, "unknown") if code is not None else None,
            "temperature_max_c":     _safe_float(_safe_get(temp_max, i)),
            "temperature_min_c":     _safe_float(_safe_get(temp_min, i)),
            "city":                  location,
            "latitude":              lat,
            "longitude":             lon,
            "is_mock":               is_mock,
            "ingested_at_utc":       ingested_at,
        })

    return records


def transform_weather(payload: dict, metadata: dict, ingested_at: str) -> list[dict]:
    """
    Transform raw Open-Meteo weather data into clean silver records.

    Combines hourly and daily records into one list, each tagged with
    record_type ('hourly' or 'daily') for downstream filtering.

    Args:
        payload:     The 'payload' from the bronze JSON.
        metadata:    The 'metadata' from the bronze JSON.
        ingested_at: The 'ingested_at_utc' timestamp from the bronze JSON.

    Returns:
        A list of flat weather record dicts.
    """
    if not isinstance(payload, dict):
        logger.warning("Unexpected payload type: %s", type(payload))
        return []

    silver_records = []

    # Flatten hourly data
    hourly = payload.get("hourly", {})
    if hourly and hourly.get("time"):
        hourly_records = _flatten_hourly(hourly, metadata, ingested_at)
        silver_records.extend(hourly_records)
        logger.info("Flattened %d hourly weather records", len(hourly_records))

    # Flatten daily data
    daily = payload.get("daily", {})
    if daily and daily.get("time"):
        daily_records = _flatten_daily(daily, metadata, ingested_at)
        silver_records.extend(daily_records)
        logger.info("Flattened %d daily weather records", len(daily_records))

    logger.info(
        "Transformed %d total weather records for %s",
        len(silver_records), metadata.get("location", "?"),
    )
    return silver_records
