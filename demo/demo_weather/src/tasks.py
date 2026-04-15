from __future__ import annotations

import json
from pathlib import Path
from typing import List

from .config import CITIES_FILE, QUEUE_NAME
from .geo import get_coordinates
from .mongo_store import save_city_weather
from .radish_app import app
from .weather import fetch_weekly_weather


def load_cities(file_path: Path) -> List[str]:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_record(city: str) -> dict:
    geo = get_coordinates(city)
    if geo is None:
        raise ValueError(f"City not found: {city}")

    weather = fetch_weekly_weather(
        latitude=geo["latitude"],
        longitude=geo["longitude"],
        timezone=geo["timezone"] or "auto",
    )

    return {
        "city": city,
        "resolved_name": geo["resolved_name"],
        "country": geo["country"],
        "latitude": geo["latitude"],
        "longitude": geo["longitude"],
        "timezone": geo["timezone"],
        "source": "open-meteo",
        "source_type": "7_day_forecast",
        "daily": weather.get("daily_records", []),
    }


@app.task(
    name="fetch_weather_for_city",
    queue=QUEUE_NAME,
    max_retries=2,#2
    retry_delay_ms=1000,
)
def fetch_weather_for_city(city: str) -> dict:
    record = build_record(city)
    save_city_weather(record)

    return {
        "city": city,
        "status": "saved",
        "daily_count": len(record["daily"]),
    }