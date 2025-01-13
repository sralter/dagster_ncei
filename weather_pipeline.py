# dagster_weather/weather_pipeline.py

import requests
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
from dagster import op, job, Out, Field, In, MetadataValue, TypeCheck
from dagster import DagsterType, Noneable
import logging
from typing import Mapping, Union, Dict
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

def get_session_with_retries():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def is_valid_email(email: str) -> bool:
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))

def parse_latlon(file_path: Path) -> Dict[str, float]:
    """Parse latitude and longitude from the file."""
    with file_path.open('r') as file:
        latlon = file.read().strip()
    latlon_dict = {
        key_value.split(': ')[0]: float(key_value.split(': ')[1])
        for key_value in latlon.split(', ')
    }
    latitude, longitude = latlon_dict['lat'], latlon_dict['lon']
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        raise ValueError(f"Invalid coordinates: lat={latitude}, lon={longitude}")
    return {"latitude": latitude, "longitude": longitude}

def read_email(file_path: Path) -> str:
    """Read and validate email from the file."""
    with file_path.open('r') as file:
        email = file.read().strip()
    if not is_valid_email(email):
        raise ValueError(f"Invalid email address: {email}")
    return email

# Define a custom type for Dagster to recognize Dict[str, Union[float, str]]
LatLonEmailType = DagsterType(
    name="LatLonEmailType",
    type_check_fn=lambda context, value: isinstance(value, dict) and all(
        isinstance(k, str) and isinstance(v, (float, str)) for k, v in value.items()
    ),
    description="Dictionary containing latitude, longitude, and email information.",
)

@op(
    config_schema={
        "latlon_file": Field(str, description="Path to the latitude and longitude file."),
        "email_file": Field(str, description="Path to the email file."),
    },
    out=Out(LatLonEmailType, description="Latitude, longitude, and email information."),
)
def fetch_information(context) -> dict:
    """
    Fetches latitude, longitude, and email information from specified files.
    """
    try:
        latlon_file = Path(context.op_config["latlon_file"])
        email_file = Path(context.op_config["email_file"])

        if not latlon_file.exists():
            raise FileNotFoundError(f"LatLon file not found: {latlon_file}")
        if not email_file.exists():
            raise FileNotFoundError(f"Email file not found: {email_file}")

        logger.info(f"Reading latitude and longitude from {latlon_file}")
        latlon = parse_latlon(latlon_file)

        logger.info(f"Reading email address from {email_file}")
        email = read_email(email_file)

        logger.info(f"Fetched information: {latlon}, email={email}")
        return {**latlon, "email": email}

    except Exception as e:
        logger.error(f"Error in fetch_information: {e}")
        raise

@op(
    ins={"info": In(LatLonEmailType, description="Latitude, longitude, and email information.")},
    out={"metadata": Out(dict, description="Metadata fetched from the weather API.")},
)
def fetch_metadata(info: dict) -> dict:
    lat = info['latitude']
    lon = info['longitude']
    email = info['email']
    url = f'https://api.weather.gov/points/{lat},{lon}'
    headers = {"User-Agent": f"MyWeatherApp/1.0 ({email})"}
    try:
        logger.info(f"Fetching metadata for coordinates: ({lat}, {lon})")
        session = get_session_with_retries()
        response = session.get(url, headers=headers)
        response.raise_for_status()
        metadata = response.json()
        metadata["email"] = email
        return metadata
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch metadata: {e}")
        raise

@op
def fetch_forecast(metadata: dict) -> dict:
    if 'properties' not in metadata or 'forecast' not in metadata['properties']:
        logger.error("Missing 'forecast' in metadata properties")
        raise KeyError("API response structure unexpected.")
    logger.info("Fetching weather forecast data")
    forecast_url = metadata['properties']['forecast']
    email = metadata["email"]
    headers = {"User-Agent": f"MyWeatherApp/1.0 ({email})"}
    response = requests.get(forecast_url, headers=headers)
    response.raise_for_status()
    return response.json()

@op
def parse_forecast(forecast_data: dict) -> dict:
    logger.info("Parsing forecast data")
    periods = forecast_data['properties']['periods']
    temperatures = [p['temperature'] for p in periods]
    times = [
        datetime.fromisoformat(p['startTime'].replace("Z", "")).strftime('%Y-%m-%d %H:%M:%S') 
        for p in periods
    ]
    return {"temperatures": temperatures, "times": times}

@op
def plot_temperature(forecast: dict, latitude: float, longitude: float, save_location: Noneable = None):
    if save_location is None:
        save_location = f"forecast_plot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    save_path = Path(save_location)
    save_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
    logger.info(f"Plotting forecast data to {save_location}")

    temperatures = forecast['temperatures']
    times = forecast['times']
    
    fig, ax = plt.subplots(figsize=(max(15, len(times) * 0.6), 6))
    ax.plot(times, temperatures, marker='o', color='cornflowerblue')
    
    formatted_times = [time.replace(' ', '\n') for time in times]
    step = max(1, len(times) // 10)
    ax.set_xticks(times[::step])
    ax.set_xticklabels(formatted_times[::step], rotation=20)
    
    plt.suptitle(f'Seven-Day Temperature Forecast\nLatitude: {latitude}, Longitude: {longitude}', 
                 y=1.0, fontsize=16)
    plt.title(f'Forecast starting at {times[0]}', fontstyle='italic', y=1, fontsize=10)
    plt.grid(visible=True)

    plt.savefig(save_location)
    plt.show()

@job
def weather_forecast_pipeline():
    info = fetch_information()
    metadata = fetch_metadata(info=info)
    forecast_data = fetch_forecast(metadata=metadata)
    forecast = parse_forecast(forecast_data)
    plot_temperature(forecast, latitude=info["latitude"], longitude=info["longitude"])