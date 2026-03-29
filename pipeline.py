import json
import os
import re
import time
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests

# =========================
# KONFIGURATION
# =========================
BUCKET_NAME = "airoinsights"
S3_PREFIX = "raw"
EXCEL_FILE = "european_cities_airports_100plus.xlsx"

AIRLABS_API_KEY = os.getenv("AIRLABS_API_KEY")
TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY")

s3 = boto3.client("s3")


# =========================
# HELPER
# =========================
def sanitize_value(value):
    text = str(value).strip().lower()
    text = text.replace("&", "and")
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_\-]", "", text)
    return text


def upload_to_s3(bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"Gespeichert: s3://{bucket}/{key}")


def make_s3_key(source, city, country, iata):
    now = datetime.now(timezone.utc)
    city_safe = sanitize_value(city)
    country_safe = sanitize_value(country)
    iata_safe = sanitize_value(iata)

    return (
        f"{S3_PREFIX}/{source}/"
        f"country={country_safe}/city={city_safe}/iata={iata_safe}/"
        f"year={now:%Y}/month={now:%m}/day={now:%d}/"
        f"{source}_{now:%Y-%m-%dT%H-%M-%SZ}.json"
    )


def build_metadata(row):
    return {
        "city": row["City"],
        "country": row["Country"],
        "airport": row["Airport"],
        "iata": row["IATA"],
        "latitude": float(row["Latitude"]),
        "longitude": float(row["Longitude"]),
    }


def country_code_from_name(country_name):
    mapping = {
        "Switzerland": "CH",
        "Germany": "DE",
        "France": "FR",
        "UK": "GB",
        "Spain": "ES",
        "Italy": "IT",
        "Austria": "AT",
        "Netherlands": "NL",
        "Belgium": "BE",
        "Czech Republic": "CZ",
        "Poland": "PL",
        "Hungary": "HU",
        "Denmark": "DK",
        "Sweden": "SE",
        "Norway": "NO",
        "Finland": "FI",
        "Portugal": "PT",
        "Ireland": "IE",
        "Greece": "GR",
        "Turkey": "TR",
        "Iceland": "IS",
        "Croatia": "HR",
        "Serbia": "RS",
        "Bulgaria": "BG",
        "Romania": "RO",
        "Estonia": "EE",
        "Latvia": "LV",
        "Lithuania": "LT",
        "Luxembourg": "LU",
        "Malta": "MT",
        "Cyprus": "CY",
    }
    return mapping.get(country_name, "")


# =========================
# AIRLABS
# =========================
def fetch_flights(iata):
    if not AIRLABS_API_KEY:
        raise ValueError(
            "AIRLABS_API_KEY fehlt. "
            "Setze ihn mit: export AIRLABS_API_KEY='DEIN_KEY'"
        )

    url = "https://airlabs.co/api/v9/schedules"
    params = {
        "api_key": AIRLABS_API_KEY,
        "dep_iata": iata
    }

    response = requests.get(url, params=params, timeout=30)
    print(f"Flights {iata}: HTTP {response.status_code}")

    if response.status_code != 200:
        print("Flights Fehlerantwort:", response.text[:500])

    response.raise_for_status()
    data = response.json()

    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"AirLabs API-Fehler bei {iata}: {data['error']}")

    return data


# =========================
# OPEN-METEO
# =========================
def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min",
        "timezone": "auto"
    }

    response = requests.get(url, params=params, timeout=30)
    print(f"Weather {lat},{lon}: HTTP {response.status_code}")

    if response.status_code != 200:
        print("Weather Fehlerantwort:", response.text[:500])

    response.raise_for_status()
    return response.json()


# =========================
# TICKETMASTER
# =========================
def fetch_events(city, country):
    if not TICKETMASTER_API_KEY:
        raise ValueError(
            "TICKETMASTER_API_KEY fehlt. "
            "Setze ihn mit: export TICKETMASTER_API_KEY='DEIN_KEY'"
        )

    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        "apikey": TICKETMASTER_API_KEY,
        "city": city,
        "countryCode": country_code_from_name(country),
        "size": 20,
        "sort": "date,asc"
    }

    response = requests.get(url, params=params, timeout=30)
    print(f"Events {city}: HTTP {response.status_code}")

    if response.status_code != 200:
        print("Events Fehlerantwort:", response.text[:500])

    response.raise_for_status()
    return response.json()


# =========================
# VERARBEITUNG
# =========================
def process_row(row):
    metadata = build_metadata(row)

    city = metadata["city"]
    country = metadata["country"]
    iata = metadata["iata"]
    lat = metadata["latitude"]
    lon = metadata["longitude"]

    print(f"\nVerarbeite: {city}, {country} ({iata})")

    # Flights
    try:
        flights_payload = fetch_flights(iata)
        flights_object = {
            "source": "airlabs",
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata,
            "payload": flights_payload
        }
        flights_key = make_s3_key("flights", city, country, iata)
        upload_to_s3(BUCKET_NAME, flights_key, flights_object)
    except Exception as e:
        print(f"Flights Fehler bei {city} ({iata}): {e}")

    # Weather
    try:
        weather_payload = fetch_weather(lat, lon)
        weather_object = {
            "source": "open_meteo",
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata,
            "payload": weather_payload
        }
        weather_key = make_s3_key("weather", city, country, iata)
        upload_to_s3(BUCKET_NAME, weather_key, weather_object)
    except Exception as e:
        print(f"Weather Fehler bei {city} ({iata}): {e}")

    # Events
    try:
        events_payload = fetch_events(city, country)
        events_object = {
            "source": "ticketmaster",
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata,
            "payload": events_payload
        }
        events_key = make_s3_key("events", city, country, iata)
        upload_to_s3(BUCKET_NAME, events_key, events_object)
    except Exception as e:
        print(f"Events Fehler bei {city} ({iata}): {e}")


# =========================
# MAIN
# =========================
def main():
    print("Lese Excel-Datei:", EXCEL_FILE)
    df = pd.read_excel(EXCEL_FILE)

    required_columns = ["City", "Country", "Latitude", "Longitude", "Airport", "IATA"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Fehlende Spalten in Excel: {missing}")

    # Erst klein testen
    df = df.head(5)

    print(f"Anzahl Test-Zeilen: {len(df)}")

    for _, row in df.iterrows():
        process_row(row)
        time.sleep(1)

    print("\nFertig.")


if __name__ == "__main__":
    main()
