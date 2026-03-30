import json
import os
import time
import requests
import boto3
from datetime import datetime, timezone


BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
SILVER_BUCKET = os.environ["SILVER_BUCKET"]
AVIATIONSTACK_API_KEY = os.environ["AVIATIONSTACK_API_KEY"]

s3 = boto3.client("s3")
secrets_client = boto3.client('secretsmanager')

def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return response['SecretString']
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise

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
    """
    Hive-style:
      {tier}/{source}/city={city}/date={YYYY-MM-DD}/{source}_{timestamp}.json
    """
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


# ── Aviationstack ─────────────────────────────────────────────────────────────

def fetch_aviationstack_mock():
    """Drop-in replacement for testing when quota is exhausted."""
    return [{
        "data": [
            {
                "departure": {"iata": "ZRH", "airport": "Zurich"},
                "arrival":   {"iata": "LHR", "airport": "Heathrow"},
                "flight":    {"iata": "LX316"},
                "flight_status": "active"
            },
            {
                "departure": {"iata": "CDG", "airport": "Charles de Gaulle"},
                "arrival":   {"iata": "FRA", "airport": "Frankfurt"},
                "flight":    {"iata": "AF1234"},
                "flight_status": "landed"
            },
        ]
    }]

def fetch_aviationstack(max_pages=3, limit=100):
    """
    Fetches flights from Aviationstack with graceful 429 handling.
    Free tier only supports 1 request; pagination stops on rate limit.
    API key is never logged.
    """
    url = "https://api.aviationstack.com/v1/flights"
    pages = []

    # Retrieve the API key from AWS Secrets Manager
    AVIATIONSTACK_API_KEY = get_secret("airoinsights-aviationstack-api-key")

    for page in range(max_pages):
        try:
            resp = requests.get(
                url,
                params={
                    "access_key": AVIATIONSTACK_API_KEY,
                    "limit": limit,
                    "offset": page * limit,
                },
                timeout=30,
            )

            if resp.status_code == 429:
                print(f"[aviationstack] Rate limited on page {page} — stopping pagination early.")
                break

            if resp.status_code == 401:
                raise RuntimeError("[aviationstack] Invalid API key (401 Unauthorized).")

            if not resp.ok:
                # Log status code only — never log the URL which contains the key
                print(f"[aviationstack] HTTP {resp.status_code} on page {page} — stopping.")
                break

            data = resp.json()

            if "error" in data:
                code = data["error"].get("code", "unknown")
                info = data["error"].get("info", "")
                raise RuntimeError(f"[aviationstack] API error (code={code}): {info}")

            pages.append(data)
            print(f"[aviationstack] Fetched page {page + 1}/{max_pages} "
                  f"({len(data.get('data') or [])} flights)")

            if page < max_pages - 1:
                time.sleep(2)  # be polite between pages

        except requests.Timeout:
            print(f"[aviationstack] Timeout on page {page} — stopping.")
            break
        except requests.ConnectionError:
            print(f"[aviationstack] Connection error on page {page} — stopping.")
            break

    if not pages:
        print("[aviationstack] No pages fetched — returning empty result.")

    return pages


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
    A single flight can appear in TWO city partitions
    (once for departure city, once for arrival city).
    Flights with no matching IATA on either end go to city=unknown.
    """
    city_map: dict[str, list] = {}

    for flight in flights:
        dep_iata = (flight.get("departure") or {}).get("iata", "")
        arr_iata = (flight.get("arrival") or {}).get("iata", "")

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
    max_pages     = event.get("max_pages", 1)  # default 1 — safe for free tier

    now = datetime.now(timezone.utc)
    results = []

    # ── Flights → Bronze ──────────────────────────────────────────────────────
    use_mock = event.get("use_mock", False)

    if fetch_flights:
        pages = fetch_aviationstack_mock() if use_mock else fetch_aviationstack(max_pages=max_pages)
        # pages = fetch_aviationstack(max_pages=max_pages)
        all_flights = [f for page in pages for f in (page.get("data") or [])]

        if all_flights:
            city_groups = group_flights_by_city(all_flights)

            for city, city_flights in city_groups.items():
                key = make_s3_key("flights", date=now, city=city, tier="raw")
                upload_to_s3(
                    BRONZE_BUCKET, key,
                    wrap_payload(
                        "aviationstack_flights",
                        city_flights,
                        {
                            "city": city,
                            "flight_count": len(city_flights),
                            "pages_fetched": len(pages),
                        },
                    ),
                )
                results.append({
                    "type": "flights",
                    "city": city,
                    "count": len(city_flights),
                    "s3_key": key,
                })
        else:
            print("[flights] No flight data returned — skipping S3 upload.")

    # ── Weather → Bronze ──────────────────────────────────────────────────────
    for loc in locations:
        try:
            raw = fetch_open_meteo(loc["name"], loc["lat"], loc["lon"])
            key = make_s3_key("weather", date=now, city=loc["name"], tier="raw")
            upload_to_s3(
                BRONZE_BUCKET, key,
                wrap_payload(
                    "open_meteo_weather",
                    raw,
                    {
                        "location": loc["name"],
                        "lat": loc["lat"],
                        "lon": loc["lon"],
                    },
                ),
            )
            results.append({
                "type": "weather",
                "location": loc["name"],
                "s3_key": key,
            })
        except Exception as e:
            print(f"[weather] Failed for {loc.get('name')}: {e}")
            results.append({
                "type": "weather",
                "location": loc.get("name"),
                "error": str(e),
            })

    return {"status": "ok", "saved": results}