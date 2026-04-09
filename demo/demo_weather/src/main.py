from __future__ import annotations

import json
import time
from pathlib import Path
from typing import List

from .config import CITIES_FILE
from .geo import get_coordinates
from .mongo_store import save_city_weather
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


def main() -> None:
    start = time.time()
    cities = load_cities(CITIES_FILE)

    for city in cities:
        record = build_record(city)
        save_city_weather(record)
        print(f"Saved weather data for {city}")

    end = time.time()
    print(f"Total execution time: {end - start:.2f} seconds")


if __name__ == "__main__":
    main()