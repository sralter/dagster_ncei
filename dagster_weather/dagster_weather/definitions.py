from dagster import Definitions, load_assets_from_modules
from dagster_weather.weather_pipeline import weather_forecast_pipeline
from dagster_weather import assets

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Combine assets and the pipeline into a single Definitions object
defs = Definitions(
    jobs=[weather_forecast_pipeline],  # Add your pipeline(s) here
    assets=all_assets,                 # Include all loaded assets
)
