"""
Simplified weather data fetching operations.
"""

import json
import os
from typing import Any, Dict

import pandas as pd
import requests
from dagster import OpExecutionContext, op


def load_data_sources():
    """Load data sources from config file."""
    config_path = "/opt/dagster/app/config/data_sources.json"
    if not os.path.exists(config_path):
        # Fallback for local development
        config_path = "config/data_sources.json"

    with open(config_path, "r") as f:
        return json.load(f)


@op(description="Fetch weather data from all configured sources")
def fetch_all_weather_data(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Fetch weather data from all enabled API sources.

    Returns:
        Dict containing weather data for all sources
    """
    context.log.info("Starting to fetch weather data from all sources")

    config = load_data_sources()
    results = {}

    for source in config.get("data_sources", []):
        if not source.get("enabled", False):
            context.log.info(
                f"Skipping disabled source: {source.get('name', 'unknown')}"
            )
            continue

        source_id = source["id"]
        context.log.info(f"Fetching data for: {source.get('name', source_id)}")

        try:
            # Simple API call for Open-Meteo
            params = {
                "latitude": source["api_config"]["latitude"],
                "longitude": source["api_config"]["longitude"],
                "hourly": ",".join(source["api_config"]["parameters"]),
                "past_days": 1,  # Get last 24 hours
                "timezone": "UTC",
            }

            response = requests.get(
                source["api_config"]["base_url"], params=params, timeout=30
            )
            response.raise_for_status()

            api_data = response.json()

            # Convert to DataFrame
            if "hourly" in api_data:
                df = pd.DataFrame(api_data["hourly"])
                df["source_id"] = source_id
                df["latitude"] = source["api_config"]["latitude"]
                df["longitude"] = source["api_config"]["longitude"]

                results[source_id] = {
                    "dataframe": df,
                    "config": source,
                    "raw_data": api_data,
                }

                context.log.info(
                    f"✓ Successfully fetched {len(df)} records for {source_id}"
                )
            else:
                context.log.warning(f"No hourly data found for {source_id}")
                results[source_id] = {"error": "No hourly data", "config": source}

        except Exception as e:
            context.log.error(f"✗ Error fetching data for {source_id}: {e}")
            results[source_id] = {"error": str(e), "config": source}

    context.log.info(f"Completed fetching data for {len(results)} sources")
    return results


@op(description="Fetch weather data from a single source")
def fetch_weather_data(context: OpExecutionContext, source_id: str) -> pd.DataFrame:
    """
    Fetch weather data from a single configured source.

    Args:
        source_id: ID of the data source to fetch from

    Returns:
        pandas DataFrame with weather data
    """
    context.log.info(f"Fetching weather data for source: {source_id}")

    config = load_data_sources()

    # Find the source
    source = None
    for s in config.get("data_sources", []):
        if s["id"] == source_id:
            source = s
            break

    if not source:
        raise ValueError(f"Data source not found: {source_id}")

    if not source.get("enabled", False):
        raise ValueError(f"Data source is disabled: {source_id}")

    try:
        # API call
        params = {
            "latitude": source["api_config"]["latitude"],
            "longitude": source["api_config"]["longitude"],
            "hourly": ",".join(source["api_config"]["parameters"]),
            "past_days": 1,
            "timezone": "UTC",
        }

        response = requests.get(
            source["api_config"]["base_url"], params=params, timeout=30
        )
        response.raise_for_status()

        api_data = response.json()

        if "hourly" in api_data:
            df = pd.DataFrame(api_data["hourly"])
            df["source_id"] = source_id
            df["latitude"] = source["api_config"]["latitude"]
            df["longitude"] = source["api_config"]["longitude"]

            context.log.info(f"Successfully fetched {len(df)} records")
            return df
        else:
            raise ValueError("No hourly data found in API response")

    except Exception as e:
        context.log.error(f"Error fetching data for {source_id}: {e}")
        raise
