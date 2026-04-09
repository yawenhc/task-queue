from __future__ import annotations

from typing import Any, Dict, List

import requests

from .config import FORECAST_BASE_URL


WEATHER_CODE_MAP = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}


def get_weather_description(weather_code: int) -> str:
    return WEATHER_CODE_MAP.get(weather_code, f"Unknown ({weather_code})")


def reshape_daily_weather(daily: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    dates = daily.get("time", [])
    weather_codes = daily.get("weathercode", [])
    max_temps = daily.get("temperature_2m_max", [])
    min_temps = daily.get("temperature_2m_min", [])
    precipitation_sums = daily.get("precipitation_sum", [])
    max_wind_speeds = daily.get("windspeed_10m_max", [])

    day_count = len(dates)
    records: List[Dict[str, Any]] = []

    for i in range(day_count):
        weather_code = weather_codes[i] if i < len(weather_codes) else None

        record = {
            "date": dates[i],
            "weather_code": weather_code,
            "weather_description": get_weather_description(weather_code) if weather_code is not None else None,
            "temperature_2m_max": max_temps[i] if i < len(max_temps) else None,
            "temperature_2m_min": min_temps[i] if i < len(min_temps) else None,
            "precipitation_sum": precipitation_sums[i] if i < len(precipitation_sums) else None,
            "windspeed_10m_max": max_wind_speeds[i] if i < len(max_wind_speeds) else None,
        }
        records.append(record)

    return records


def fetch_weekly_weather(latitude: float, longitude: float, timezone: str = "auto") -> Dict[str, Any]:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": (
            "weathercode,"
            "temperature_2m_max,"
            "temperature_2m_min,"
            "precipitation_sum,"
            "windspeed_10m_max"
        ),
        "timezone": timezone,
        "forecast_days": 7,
    }

    response = requests.get(FORECAST_BASE_URL, params=params, timeout=20)
    response.raise_for_status()

    payload = response.json()
    payload["daily_records"] = reshape_daily_weather(payload.get("daily", {}))
    return payload