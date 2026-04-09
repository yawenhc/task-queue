from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from .config import GEOCODING_BASE_URL


def get_coordinates(city_name: str) -> Optional[Dict[str, Any]]:
    params = {
        "name": city_name,
        "count": 1,
        "language": "en",
        "format": "json",
    }

    response = requests.get(GEOCODING_BASE_URL, params=params, timeout=15)
    response.raise_for_status()

    payload = response.json()
    results = payload.get("results")

    if not results:
        return None

    first = results[0]
    return {
        "city": city_name,
        "resolved_name": first.get("name"),
        "country": first.get("country"),
        "latitude": first.get("latitude"),
        "longitude": first.get("longitude"),
        "timezone": first.get("timezone"),
    }