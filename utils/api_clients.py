"""
API client utilities for fetching data from various weather APIs.
"""

import logging

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from utils.config_loader import ApiConfig, DataSourceConfig, get_config_loader

logger = logging.getLogger(__name__)


class WeatherApiClient:
    """Base class for weather API clients."""

    def __init__(self, config: DataSourceConfig, api_config: ApiConfig):
        """Initialize the API client.

        Args:
            config: Data source configuration
            api_config: API configuration settings
        """
        self.config = config
        self.api_config = api_config
        self.setup_client()

    def setup_client(self):
        """Setup the API client with caching and retry logic."""
        cache_session = requests_cache.CachedSession(
            ".cache", expire_after=self.api_config.cache_ttl_seconds
        )
        retry_session = retry(
            cache_session,
            retries=self.api_config.retry_attempts,
            backoff_factor=self.api_config.retry_backoff_factor,
        )
        self.client = openmeteo_requests.Client(session=retry_session)

    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from the API.

        Returns:
            DataFrame with fetched data
        """
        raise NotImplementedError("Subclasses must implement fetch_data")


class OpenMeteoClient(WeatherApiClient):
    """Client for Open-Meteo weather API."""

    def fetch_data(self) -> pd.DataFrame:
        """Fetch weather data from Open-Meteo API.

        Returns:
            DataFrame with weather data
        """
        logger.info(f"Fetching data for {self.config.name} ({self.config.id})")

        api_config = self.config.api_config

        # Build API parameters
        params = {
            "latitude": api_config["latitude"],
            "longitude": api_config["longitude"],
            "hourly": api_config.get("hourly_params", ["temperature_2m"]),
            "timezone": api_config.get("timezone", "UTC"),
            "forecast_days": api_config.get("forecast_days", 1),
        }

        # Add daily parameters if specified
        if api_config.get("daily_params"):
            params["daily"] = api_config["daily_params"]

        logger.info(f"API request params: {params}")

        try:
            # Fetch data from API
            responses = self.client.weather_api(api_config["base_url"], params=params)

            # Process first location
            response = responses[0]
            logger.info(
                f"API response - Coordinates: {response.Latitude()}°N {response.Longitude()}°E"
            )
            logger.info(f"Elevation: {response.Elevation()} m asl")
            logger.info(
                f"Timezone: {response.Timezone()} {response.TimezoneAbbreviation()}"
            )

            # Process hourly data
            hourly = response.Hourly()

            # Create DataFrame with timestamp
            hourly_data = {
                "timestamp": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s"),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left",
                )
            }

            # Add each requested parameter
            for i, param in enumerate(params["hourly"]):
                hourly_data[param] = hourly.Variables(i).ValuesAsNumpy()

            # Add location metadata
            hourly_data.update(
                {
                    "latitude": response.Latitude(),
                    "longitude": response.Longitude(),
                    "location_name": api_config["location_name"],
                }
            )

            df = pd.DataFrame(data=hourly_data)

            logger.info(
                f"Successfully fetched {len(df)} records for {self.config.name}"
            )
            logger.info(f"Data columns: {list(df.columns)}")
            logger.info(
                f"Data date range: {df['timestamp'].min()} to {df['timestamp'].max()}"
            )

            return df

        except Exception as e:
            logger.error(f"Error fetching data for {self.config.name}: {str(e)}")
            raise


class ApiClientFactory:
    """Factory for creating API clients based on configuration."""

    @staticmethod
    def create_client(data_source: DataSourceConfig) -> WeatherApiClient:
        """Create an API client for the given data source.

        Args:
            data_source: Data source configuration

        Returns:
            Appropriate API client instance

        Raises:
            ValueError: If API type is not supported
        """
        config_loader = get_config_loader()
        api_config = config_loader.get_api_config()

        if data_source.api_type == "openmeteo":
            return OpenMeteoClient(data_source, api_config)
        else:
            raise ValueError(f"Unsupported API type: {data_source.api_type}")


def get_api_client(source_id: str) -> WeatherApiClient:
    """Get an API client for a specific data source.

    Args:
        source_id: ID of the data source

    Returns:
        API client instance

    Raises:
        ValueError: If data source is not found or not enabled
    """
    config_loader = get_config_loader()
    data_source = config_loader.get_data_source(source_id)

    if not data_source:
        raise ValueError(f"Data source not found: {source_id}")

    if not data_source.enabled:
        raise ValueError(f"Data source is disabled: {source_id}")

    return ApiClientFactory.create_client(data_source)
