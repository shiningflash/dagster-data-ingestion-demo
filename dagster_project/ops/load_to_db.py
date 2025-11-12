"""
Op to load weather data into PostgreSQL database.
"""

import os

import pandas as pd
from dagster import OpExecutionContext, op
from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Float, MetaData, Table, create_engine, text
from sqlalchemy.ext.declarative import declarative_base

# Load environment variables
load_dotenv()

Base = declarative_base()


def get_database_engine():
    """Create and return SQLAlchemy database engine."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    engine = create_engine(database_url)
    return engine


def create_weather_table(engine):
    """Create the weather_data table if it doesn't exist."""
    metadata = MetaData()

    weather_table = Table(
        "weather_data",
        metadata,
        Column("timestamp", DateTime, primary_key=True),
        Column("temperature_2m", Float, nullable=False),
        Column("latitude", Float, nullable=False),
        Column("longitude", Float, nullable=False),
    )

    # Create table if it doesn't exist
    metadata.create_all(engine)
    return weather_table


@op(description="Load transformed weather data into PostgreSQL database")
def load_weather_to_db(
    context: OpExecutionContext, transformed_data: pd.DataFrame
) -> str:
    """
    Load the transformed weather data into PostgreSQL database.

    Args:
        transformed_data: Transformed weather DataFrame

    Returns:
        Success message with record count
    """
    context.log.info(
        f"Starting database load of {len(transformed_data)} weather records"
    )

    try:
        # Create database engine
        engine = get_database_engine()
        context.log.info("Successfully connected to PostgreSQL database")

        # Create table if it doesn't exist
        create_weather_table(engine)
        context.log.info("Weather table created or verified to exist")

        # Check if we have data to insert
        if transformed_data.empty:
            context.log.warning("No data to insert - DataFrame is empty")
            return "No data to insert"

        # Clear existing data for today (optional - remove if you want to accumulate data)
        with engine.connect() as connection:
            # Get today's date range from the new data
            min_date = transformed_data["timestamp"].min()
            max_date = transformed_data["timestamp"].max()

            context.log.info(f"Data date range: {min_date} to {max_date}")

            # Delete existing data for the same date range to avoid duplicates
            delete_query = text(
                "DELETE FROM weather_data WHERE timestamp >= :min_date AND timestamp <= :max_date"
            )
            result = connection.execute(
                delete_query, {"min_date": min_date, "max_date": max_date}
            )
            connection.commit()

            if result.rowcount > 0:
                context.log.info(
                    f"Deleted {result.rowcount} existing records in the date range"
                )

        # Insert new data using pandas to_sql method
        transformed_data.to_sql(
            name="weather_data",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        context.log.info(
            f"Successfully inserted {len(transformed_data)} records into weather_data table"
        )

        # Verify the insertion by counting total records
        with engine.connect() as connection:
            count_query = text("SELECT COUNT(*) as count FROM weather_data")
            result = connection.execute(count_query)
            total_records = result.fetchone()[0]
            context.log.info(f"Total records in weather_data table: {total_records}")

            # Show a sample of the inserted data
            sample_query = text(
                "SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 3"
            )
            sample_result = connection.execute(sample_query)
            context.log.info("Sample of inserted data:")
            for row in sample_result:
                context.log.info(
                    f"  {row.timestamp} | {row.temperature_2m}°C | ({row.latitude}, {row.longitude})"
                )

        engine.dispose()

        success_message = (
            f"Successfully loaded {len(transformed_data)} weather records to database"
        )
        context.log.info(success_message)
        return success_message

    except Exception as e:
        context.log.error(f"Error loading data to database: {str(e)}")
        raise
