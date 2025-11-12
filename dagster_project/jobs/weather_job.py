"""
Simple weather ETL job - maintainable and workable.
"""

import os

import pandas as pd
import requests
from dagster import job, op
from sqlalchemy import create_engine, text


@op
def fetch_weather_data():
    """Fetch weather data from Open-Meteo API for Berlin."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.5244,  # Berlin
        "longitude": 13.4105,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min",
        "timezone": "Europe/Berlin",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


@op
def transform_weather_data(context, weather_data):
    """Transform weather data into pandas DataFrame."""
    hourly = weather_data.get("hourly", {})

    # Convert hourly data to DataFrame
    df_hourly = pd.DataFrame(
        {
            "time": hourly.get("time", []),
            "temperature": hourly.get("temperature_2m", []),
            "humidity": hourly.get("relative_humidity_2m", []),
            "wind_speed": hourly.get("wind_speed_10m", []),
        }
    )

    if not df_hourly.empty:
        df_hourly["time"] = pd.to_datetime(df_hourly["time"])
        df_hourly["location"] = "Berlin"

    context.log.info(f"Transformed {len(df_hourly)} hourly weather records")
    return df_hourly


@op
def load_weather_to_db(context, weather_df):
    """Load weather data to PostgreSQL database."""
    if weather_df.empty:
        context.log.warning("No data to load")
        return

    # Database connection
    db_url = f"postgresql://{os.getenv('POSTGRES_USER', 'dagster')}:{os.getenv('POSTGRES_PASSWORD', 'dagster')}@{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'dagster')}"

    engine = create_engine(db_url)

    # Drop and recreate table to ensure correct schema
    drop_and_create_table_sql = """
    DROP TABLE IF EXISTS weather_data;
    CREATE TABLE weather_data (
        id SERIAL PRIMARY KEY,
        time TIMESTAMP,
        location VARCHAR(100),
        temperature FLOAT,
        humidity FLOAT,
        wind_speed FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with engine.begin() as conn:
        conn.execute(text(drop_and_create_table_sql))

        # Debug: log DataFrame info
        context.log.info(f"DataFrame columns: {list(weather_df.columns)}")
        context.log.info(f"DataFrame shape: {weather_df.shape}")
        context.log.info(f"DataFrame dtypes: {weather_df.dtypes.to_dict()}")

        # Insert data
        weather_df.to_sql(
            "weather_data", conn, if_exists="append", index=False, method="multi"
        )

    context.log.info(f"Loaded {len(weather_df)} weather records to database")


@job(description="Simple weather ETL pipeline for Berlin")
def weather_etl_job():
    """
    Simple Weather ETL Job:
    1. Fetches current weather data from Open-Meteo API
    2. Transforms data into structured format
    3. Loads data into PostgreSQL database
    """
    raw_data = fetch_weather_data()
    transformed_data = transform_weather_data(raw_data)
    load_weather_to_db(transformed_data)
