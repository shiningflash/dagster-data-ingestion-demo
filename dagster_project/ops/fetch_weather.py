"""
Op to fetch weather data from Open-Meteo API.
"""

import openmeteo_requests
import pandas as pd
import requests_cache
from dagster import OpExecutionContext, op
from retry_requests import retry


@op(description="Fetch hourly temperature data from Open-Meteo API")
def fetch_weather_data(context: OpExecutionContext) -> pd.DataFrame:
    """
    Fetch hourly temperature data from Open-Meteo API for Berlin.
    Returns a pandas DataFrame with timestamp, temperature_2m, latitude, longitude.
    """
    context.log.info(
        "Setting up Open-Meteo client with caching and retry functionality"
    )

    # Setup the Open-meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # API parameters for Berlin, Germany
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.52,  # Berlin latitude
        "longitude": 13.405,  # Berlin longitude
        "hourly": ["temperature_2m"],
        "forecast_days": 1,  # Get data for today
    }

    context.log.info(
        f"Fetching weather data for Berlin (lat: {params['latitude']}, lon: {params['longitude']})"
    )

    try:
        # Fetch data from API
        responses = openmeteo.weather_api(url, params=params)

        # Process first location
        response = responses[0]
        context.log.info(
            f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E"
        )
        context.log.info(f"Elevation: {response.Elevation()} m asl")
        context.log.info(
            f"Timezone: {response.Timezone()} {response.TimezoneAbbreviation()}"
        )
        context.log.info(
            f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()} s"
        )

        # Process hourly data
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

        # Create DataFrame
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s"),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            ),
            "temperature_2m": hourly_temperature_2m,
            "latitude": response.Latitude(),
            "longitude": response.Longitude(),
        }

        df = pd.DataFrame(data=hourly_data)

        context.log.info(f"Successfully fetched {len(df)} weather records")
        context.log.info(f"Data shape: {df.shape}")
        context.log.info(
            f"Temperature range: {df['temperature_2m'].min():.1f}°C to {df['temperature_2m'].max():.1f}°C"
        )

        return df

    except Exception as e:
        context.log.error(f"Error fetching weather data: {str(e)}")
        raise
