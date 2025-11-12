"""
Op to transform weather data.
"""

import pandas as pd
from dagster import OpExecutionContext, op


@op(description="Transform weather data by cleaning and formatting")
def transform_weather_data(
    context: OpExecutionContext, raw_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform the raw weather data:
    - Rename 'date' column to 'timestamp'
    - Ensure proper data types
    - Remove any null values
    - Round temperature to 1 decimal place

    Args:
        raw_data: Raw weather DataFrame from fetch_weather_data op

    Returns:
        Transformed DataFrame ready for database insertion
    """
    context.log.info(f"Starting transformation of {len(raw_data)} weather records")

    try:
        # Create a copy to avoid modifying the original data
        df = raw_data.copy()

        # Rename 'date' column to 'timestamp' for database consistency
        df = df.rename(columns={"date": "timestamp"})

        # Ensure proper data types
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["temperature_2m"] = pd.to_numeric(df["temperature_2m"])
        df["latitude"] = pd.to_numeric(df["latitude"])
        df["longitude"] = pd.to_numeric(df["longitude"])

        # Round temperature to 1 decimal place
        df["temperature_2m"] = df["temperature_2m"].round(1)

        # Remove any records with null values
        initial_count = len(df)
        df = df.dropna()
        final_count = len(df)

        if initial_count != final_count:
            context.log.info(
                f"Removed {initial_count - final_count} records with null values"
            )

        # Log transformation results
        context.log.info("Transformation completed successfully")
        context.log.info(f"Final data shape: {df.shape}")
        context.log.info(f"Columns: {list(df.columns)}")
        context.log.info("Data types:")
        for col, dtype in df.dtypes.items():
            context.log.info(f"  {col}: {dtype}")

        context.log.info("Sample data:")
        context.log.info(f"  First timestamp: {df['timestamp'].iloc[0]}")
        context.log.info(f"  Last timestamp: {df['timestamp'].iloc[-1]}")
        context.log.info(
            f"  Temperature range: {df['temperature_2m'].min()}°C to {df['temperature_2m'].max()}°C"
        )
        context.log.info(
            f"  Location: ({df['latitude'].iloc[0]}, {df['longitude'].iloc[0]})"
        )

        return df

    except Exception as e:
        context.log.error(f"Error during data transformation: {str(e)}")
        raise
