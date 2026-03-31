import json
import os
import time
import requests
import boto3
from datetime import datetime, timezone


BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
SILVER_BUCKET = os.environ["SILVER_BUCKET"]

s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")


def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return response["SecretString"]
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise


# Fetch API key globally during cold start
try:
    AIRLABS_API_KEY = get_secret("airoinsights-airlabs-api-key")
except Exception:
    AIRLABS_API_KEY = os.environ.get("AIRLABS_API_KEY", "")


# ── S3 helpers ────────────────────────────────────────────────────────────────

def upload_to_s3(bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Saved: s3://{bucket}/{key}")


def make_s3_key(source: str, date: datetime, city: str = None, tier: str = "raw") -> str:
    date_partition = f"date={date:%Y-%m-%d}"
    partition_path = f"city={city}/{date_partition}" if city else date_partition
    filename = f"{source}_{date:%Y-%m-%dT%H-%M-%SZ}.json"
    return f"{tier}/{source}/{partition_path}/{filename}"


def wrap_payload(source, payload, metadata=None):
    return {
        "source": source,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata or {},
        "payload": payload,
    }


# ── AirLabs ───────────────────────────────────────────────────────────────────

def fetch_airlabs_flights(iata: str) -> list:
    """
    Fetch departure schedules from AirLabs for a given airport IATA.
    Response is a flat list of flight objects — NOT wrapped in a key.
    Each flight has flat fields: dep_iata, arr_iata, flight_iata, status, etc.
    """
    url = "https://airlabs.co/api/v9/schedules"

    try:
        resp = requests.get(
            url,
            params={"api_key": AIRLABS_API_KEY, "dep_iata": iata},
            timeout=30,
        )

        if resp.status_code == 401:
            raise RuntimeError("[airlabs] Invalid API key (401 Unauthorized).")

        if resp.status_code == 429:
            print(f"[airlabs] Rate limited for {iata}.")
            return []

        if not resp.ok:
            print(f"[airlabs] HTTP {resp.status_code} for iata={iata} — skipping.")
            return []

        data = resp.json()

        # AirLabs wraps response in {"response": [...], "error": {...}}
        # on error, or returns {"response": [...]} on success
        if isinstance(data, dict):
            if "error" in data:
                code = data["error"].get("code", "unknown")
                info = data["error"].get("message", "")
                print(f"[airlabs] API error for {iata} (code={code}): {info}")
                return []
            # Successful response is under "response" key
            flights = data.get("response", [])
        elif isinstance(data, list):
            # Some API versions return a raw list
            flights = data
        else:
            print(f"[airlabs] Unexpected response type for {iata}: {type(data)}")
            return []

        print(f"[airlabs] Fetched {len(flights)} flights for {iata}")
        return flights

    except requests.Timeout:
        print(f"[airlabs] Timeout for iata={iata}.")
        return []
    except requests.ConnectionError:
        print(f"[airlabs] Connection error for iata={iata}.")
        return []


# ── Open-Meteo ────────────────────────────────────────────────────────────────

def fetch_open_meteo(name, lat, lon):
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
            "daily": "weather_code,temperature_2m_max,temperature_2m_min",
            "timezone": "Europe/Zurich",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ── Flight grouping ───────────────────────────────────────────────────────────

IATA_TO_CITY = {
    "ZRH": "zurich",
    "FRA": "frankfurt",
    "LHR": "london",
    "LGW": "london",
    "STN": "london",
    "CDG": "paris",
    "ORY": "paris",
}


def group_flights_by_city(flights: list[dict]) -> dict[str, list]:
    """
    AirLabs flat format: flight["dep_iata"] and flight["arr_iata"]
    A flight can appear in TWO city buckets (dep + arr).
    Unmatched flights go to city=unknown.
    """
    city_map: dict[str, list] = {}

    for flight in flights:
        # AirLabs uses flat fields, not nested dicts
        dep_iata = flight.get("dep_iata", "") or ""
        arr_iata = flight.get("arr_iata", "") or ""

        cities = set()
        if dep_iata in IATA_TO_CITY:
            cities.add(IATA_TO_CITY[dep_iata])
        if arr_iata in IATA_TO_CITY:
            cities.add(IATA_TO_CITY[arr_iata])
        if not cities:
            cities.add("unknown")

        for city in cities:
            city_map.setdefault(city, []).append(flight)

    return city_map


# ── Lambda handler ────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    locations     = event.get("locations", [])
    fetch_flights = event.get("fetch_flights", True)
    use_mock      = event.get("use_mock", False)

    now = datetime.now(timezone.utc)
    results = []

    # ── Flights → Bronze ──────────────────────────────────────────────────────
    if fetch_flights:
        for loc in locations:
            iata = loc.get("iata", "")

            if not iata:
                msg = f"Missing 'iata' for location '{loc.get('name', 'unknown')}'"
                print(f"[flights] {msg} — skipping.")
                results.append({"type": "flights", "city": loc.get("name"), "error": msg})
                continue

            try:
                if use_mock:
                    print(f"[flights] MOCK MODE for {iata}")
                    # AirLabs flat format — matches group_flights_by_city expectations
                    flights = [
                        {"flight_iata": f"MK{iata}1", "dep_iata": iata,
                         "arr_iata": "LHR", "status": "scheduled"},
                        {"flight_iata": f"MK{iata}2", "dep_iata": "CDG",
                         "arr_iata": iata, "status": "active"},
                    ]
                else:
                    flights = fetch_airlabs_flights(iata)

                if not flights:
                    print(f"[flights] No flights returned for {iata} — skipping upload.")
                    results.append({"type": "flights", "city": loc.get("name"),
                                    "iata": iata, "count": 0})
                    continue

                city_groups = group_flights_by_city(flights)

                for city, city_flights in city_groups.items():
                    key = make_s3_key("flights", date=now, city=city, tier="raw")
                    upload_to_s3(
                        BRONZE_BUCKET, key,
                        wrap_payload(
                            "airlabs_flights_mock" if use_mock else "airlabs_flights",
                            city_flights,
                            {
                                "city": city,
                                "flight_count": len(city_flights),
                                "source_iata": iata,
                                "is_mock": use_mock,
                            },
                        ),
                    )
                    results.append({
                        "type": "flights",
                        "city": city,
                        "count": len(city_flights),
                        "s3_key": key,
                        "mocked": use_mock,
                    })

            except Exception as e:
                print(f"[flights] Failed for {loc.get('name')}: {e}")
                results.append({"type": "flights", "city": loc.get("name"), "error": str(e)})

    # ── Weather → Bronze ──────────────────────────────────────────────────────
    for loc in locations:
        try:
            if use_mock:
                print(f"[weather] MOCK MODE for {loc['name']}")
                raw = {
                    "latitude": loc["lat"],
                    "longitude": loc["lon"],
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
            else:
                raw = fetch_open_meteo(loc["name"], loc["lat"], loc["lon"])

            key = make_s3_key("weather", date=now, city=loc["name"], tier="raw")
            upload_to_s3(
                BRONZE_BUCKET, key,
                wrap_payload(
                    "open_meteo_weather_mock" if use_mock else "open_meteo_weather",
                    raw,
                    {"location": loc["name"], "lat": loc["lat"],
                     "lon": loc["lon"], "is_mock": use_mock},
                ),
            )
            results.append({"type": "weather", "location": loc["name"],
                            "s3_key": key, "mocked": use_mock})

        except Exception as e:
            print(f"[weather] Failed for {loc.get('name')}: {e}")
            results.append({"type": "weather", "location": loc.get("name"), "error": str(e)})

    return {"status": "ok", "saved": results}