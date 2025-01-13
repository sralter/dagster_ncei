import requests
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
from dagster import op, job, Output, Out
import logging

logger = logging.getLogger(__name__)

@op(config_schema={"latlon_file": str, "email_file": str}, out={"lat": Out(float), "lon": Out(float), "email": Out(str)})
def fetch_information(context) -> dict:
    latlon_file = Path(context.op_config["latlon_file"])
    email_file = Path(context.op_config["email_file"])
    
    logger.info(f"Reading latitude and longitude from {latlon_file}")
    with latlon_file.open('r') as file:
        latlon = file.read().strip()
    latlon_dict = {key_value.split(': ')[0]: float(key_value.split(': ')[1])
                   for key_value in latlon.split(', ')}
    latitude, longitude = latlon_dict['lat'], latlon_dict['lon']
    
    logger.info(f"Reading email address from {email_file}")
    with email_file.open('r') as file:
        email = file.read().strip()
    
    logger.info(f"Fetched information: lat={latitude}, lon={longitude}, email={email}")
    return (
        Output(value=latitude, output_name='lat'), Output(value=longitude, output_name='lon'), Output(value=email, output_name='email')
    )

@op
def fetch_metadata(lat: float, lon: float, email: str) -> dict:
    logger.info(f"Fetching metadata for coordinates: ({lat}, {lon})")
    url = f'https://api.weather.gov/points/{lat},{lon}'
    headers = {"User-Agent": f"MyWeatherApp/1.0 ({email})"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

@op
def fetch_forecast(metadata: dict, email: str) -> dict:
    logger.info("Fetching weather forecast data")
    forecast_url = metadata['properties']['forecast']
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
def plot_temperature(forecast: dict, save_location: str = "forecast_plot.png"):
    logger.info(f"Plotting forecast data to {save_location}")
    temperatures = forecast['temperatures']
    times = forecast['times']
    
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.plot(times, temperatures, marker='o', color='cornflowerblue')
    
    formatted_times = [time.replace(' ', '\n') for time in times]
    step = max(1, len(times) // 10)  # show ~10 labels 
    ax.set_xticks(times[::step])
    ax.set_xticklabels(formatted_times[::step], rotation=20)
    
    plt.suptitle('Seven-Day Temperature Forecast\nHayden Planetarium, American Museum of Natural History, NYC', 
                 y=1.0, fontsize=16)
    plt.title(f'Forecast starting at {times[0]}', fontstyle='italic', y=1, fontsize=10)
    plt.grid(visible=True)

    plt.savefig(save_location)
    plt.show()

@job
def weather_forecast_pipeline():
    # fetch information from fetch_information
    latlon_output = fetch_information()

    # unpack the outputs as a tuple
    lat, lon, email = latlon_output

    # fetch metadata using the fetched information
    metadata = fetch_metadata(lat=lat, lon=lon, email=email)
    
    # fetch forecast using the fetched metadata
    forecast_data = fetch_forecast(metadata=metadata, email=email)
    
    # parse the forecast data
    forecast = parse_forecast(forecast_data=forecast_data)
    
    # plot the temperature data
    plot_temperature(forecast=forecast)