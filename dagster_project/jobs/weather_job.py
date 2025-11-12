"""
Dagster job definition for the weather data ETL pipeline.
"""

from dagster import job
from dagster_project.ops.fetch_weather import fetch_weather_data
from dagster_project.ops.load_to_db import load_weather_to_db
from dagster_project.ops.transform_weather import transform_weather_data


@job(
    description="ETL pipeline to fetch, transform, and load weather data from Open-Meteo API to PostgreSQL"
)
def weather_etl_job():
    """
    Weather ETL Job that orchestrates the complete pipeline:
    1. Fetch weather data from Open-Meteo API
    2. Transform the data (clean, format, validate)
    3. Load the transformed data into PostgreSQL database

    The ops are connected in sequence: fetch → transform → load
    """
    # Define the pipeline flow
    raw_data = fetch_weather_data()
    transformed_data = transform_weather_data(raw_data)
    load_weather_to_db(transformed_data)
