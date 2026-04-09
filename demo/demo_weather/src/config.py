from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CITIES_FILE = DATA_DIR / "long_cities.json"

GEOCODING_BASE_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_BASE_URL = "https://api.open-meteo.com/v1/forecast"

MONGO_URI = "mongodb://root:example@localhost:27017/?authSource=admin"
MONGO_DB_NAME = "weather_demo"
MONGO_COLLECTION_NAME = "city_weather"

BROKER_URL = "redis://localhost:6379/0"
BACKEND_URL = "redis://localhost:6379/1"
QUEUE_NAME = "default"
RESULT_EXPIRE_SECONDS = 86400
DLQ_MAX_LENGTH = 10