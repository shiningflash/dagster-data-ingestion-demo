"""
Op to transform weather data with configuration-based validation.
"""

from typing import Dict

import pandas as pd
from dagster import Config, OpExecutionContext, op
from pydantic import Field

from utils.config_loader import get_config_loader


class TransformWeatherConfig(Config):
    """Configuration for transform weather op."""

    source_id: str = Field(description="ID of the data source being transformed")


@op(description="Transform weather data by cleaning and formatting with validation")
def transform_weather_data(
    context: OpExecutionContext, config: TransformWeatherConfig, raw_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform weather data based on configuration:
    - Rename columns according to database schema
    - Ensure proper data types
    - Apply data quality validation
    - Remove invalid/null values
    - Apply business rules

    Args:
        config: Configuration containing source_id
        raw_data: Raw weather DataFrame from fetch operation

    Returns:
        Transformed DataFrame ready for database insertion
    """
    source_id = config.source_id
    context.log.info(f"Transforming weather data for source: {source_id}")

    if raw_data.empty:
        context.log.warning("No data to transform - DataFrame is empty")
        return pd.DataFrame()

    try:
        # Get configuration for data quality validation
        config_loader = get_config_loader()
        data_source = config_loader.get_data_source(source_id)
        pipeline_settings = config_loader.get_pipeline_settings()

        if not data_source:
            raise ValueError(f"Data source configuration not found: {source_id}")

        context.log.info(f"Input data shape: {raw_data.shape}")
        context.log.info(f"Input columns: {list(raw_data.columns)}")

        # Create a copy to avoid modifying the original data
        df = raw_data.copy()

        # Apply column mapping if needed (e.g., 'date' -> 'timestamp')
        if "date" in df.columns and "timestamp" not in df.columns:
            df = df.rename(columns={"date": "timestamp"})
            context.log.info("Renamed 'date' column to 'timestamp'")

        # Ensure required columns exist based on database schema
        db_columns = data_source.database["columns"]
        required_columns = list(db_columns.keys())

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            context.log.warning(f"Missing columns in data: {missing_columns}")

        # Ensure proper data types
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Process numeric columns with validation
        data_quality = pipeline_settings.get("data_quality", {})

        # Temperature validation and formatting
        if "temperature_2m" in df.columns:
            df["temperature_2m"] = pd.to_numeric(df["temperature_2m"], errors="coerce")
            df["temperature_2m"] = df["temperature_2m"].round(1)

            # Apply temperature range validation
            temp_range = data_quality.get("temperature_range", {})
            if temp_range:
                min_temp = temp_range.get("min", -50)
                max_temp = temp_range.get("max", 60)

                invalid_temp_mask = (df["temperature_2m"] < min_temp) | (
                    df["temperature_2m"] > max_temp
                )
                invalid_count = invalid_temp_mask.sum()

                if invalid_count > 0:
                    context.log.warning(
                        f"Found {invalid_count} temperature values outside range [{min_temp}, {max_temp}]°C"
                    )
                    df.loc[invalid_temp_mask, "temperature_2m"] = None

        # Humidity validation
        if "relative_humidity_2m" in df.columns:
            df["relative_humidity_2m"] = pd.to_numeric(
                df["relative_humidity_2m"], errors="coerce"
            )
            df["relative_humidity_2m"] = df["relative_humidity_2m"].round(1)

            humidity_range = data_quality.get("humidity_range", {})
            if humidity_range:
                min_humidity = humidity_range.get("min", 0)
                max_humidity = humidity_range.get("max", 100)

                invalid_humidity_mask = (df["relative_humidity_2m"] < min_humidity) | (
                    df["relative_humidity_2m"] > max_humidity
                )
                invalid_count = invalid_humidity_mask.sum()

                if invalid_count > 0:
                    context.log.warning(
                        f"Found {invalid_count} humidity values outside range [{min_humidity}, {max_humidity}]%"
                    )
                    df.loc[invalid_humidity_mask, "relative_humidity_2m"] = None

        # Wind speed validation
        if "wind_speed_10m" in df.columns:
            df["wind_speed_10m"] = pd.to_numeric(df["wind_speed_10m"], errors="coerce")
            df["wind_speed_10m"] = df["wind_speed_10m"].round(1)

            wind_range = data_quality.get("wind_speed_range", {})
            if wind_range:
                min_wind = wind_range.get("min", 0)
                max_wind = wind_range.get("max", 200)

                invalid_wind_mask = (df["wind_speed_10m"] < min_wind) | (
                    df["wind_speed_10m"] > max_wind
                )
                invalid_count = invalid_wind_mask.sum()

                if invalid_count > 0:
                    context.log.warning(
                        f"Found {invalid_count} wind speed values outside range [{min_wind}, {max_wind}] km/h"
                    )
                    df.loc[invalid_wind_mask, "wind_speed_10m"] = None

        # Handle coordinates
        for coord_col in ["latitude", "longitude"]:
            if coord_col in df.columns:
                df[coord_col] = pd.to_numeric(df[coord_col], errors="coerce")

        # Check for null values and apply null percentage validation
        initial_count = len(df)
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()

        if total_nulls > 0:
            context.log.info("Null value counts:")
            for col, null_count in null_counts.items():
                if null_count > 0:
                    percentage = (null_count / len(df)) * 100
                    context.log.info(f"  {col}: {null_count} ({percentage:.1f}%)")

        # Apply null percentage threshold
        max_null_percentage = data_quality.get("max_null_percentage", 5)
        for col, null_count in null_counts.items():
            if null_count > 0:
                percentage = (null_count / len(df)) * 100
                if percentage > max_null_percentage:
                    context.log.error(
                        f"Column {col} has {percentage:.1f}% null values, exceeding threshold of {max_null_percentage}%"
                    )
                    raise ValueError(
                        f"Data quality check failed: too many null values in {col}"
                    )

        # Remove rows with null values in critical columns
        critical_columns = ["timestamp"]
        if "temperature_2m" in df.columns:
            critical_columns.append("temperature_2m")

        df = df.dropna(subset=critical_columns)
        final_count = len(df)

        if initial_count != final_count:
            removed_count = initial_count - final_count
            context.log.info(
                f"Removed {removed_count} records with null values in critical columns"
            )

        # Sort by timestamp
        if "timestamp" in df.columns:
            df = df.sort_values("timestamp")

        # Final validation
        if df.empty:
            context.log.warning("All data was filtered out during transformation")
            return df

        # Log transformation results
        context.log.info("Transformation completed successfully")
        context.log.info(f"Final data shape: {df.shape}")
        context.log.info(f"Columns: {list(df.columns)}")
        context.log.info("Data types:")
        for col, dtype in df.dtypes.items():
            context.log.info(f"  {col}: {dtype}")

        if "timestamp" in df.columns:
            context.log.info("Sample data:")
            context.log.info(f"  First timestamp: {df['timestamp'].iloc[0]}")
            context.log.info(f"  Last timestamp: {df['timestamp'].iloc[-1]}")

        if "temperature_2m" in df.columns:
            context.log.info(
                f"  Temperature range: {df['temperature_2m'].min()}°C to {df['temperature_2m'].max()}°C"
            )

        if "location_name" in df.columns:
            context.log.info(f"  Location: {df['location_name'].iloc[0]}")

        return df

    except Exception as e:
        context.log.error(
            f"Error during data transformation for source {source_id}: {str(e)}"
        )
        raise


@op(description="Transform all weather data from multiple sources")
def transform_all_weather_data(
    context: OpExecutionContext, all_raw_data: Dict[str, pd.DataFrame]
) -> Dict[str, pd.DataFrame]:
    """
    Transform weather data from all sources using configuration-based validation.

    Args:
        all_raw_data: Dict mapping source_id to raw weather data DataFrame

    Returns:
        Dict mapping source_id to transformed weather data DataFrame
    """
    try:
        config_loader = get_config_loader()
        data_sources = config_loader.get_data_sources()
        pipeline_settings = config_loader.get_pipeline_settings()

        transformed_data = {}

        context.log.info(f"Transforming data for {len(all_raw_data)} sources")

        for source_id, raw_data in all_raw_data.items():
            if source_id not in data_sources:
                context.log.warning(f"Skipping unknown source: {source_id}")
                continue

            context.log.info(f"Transforming data for source: {source_id}")

            # Use the individual transform logic
            data_source = data_sources[source_id]

            # Create a copy to avoid modifying the original data
            df = raw_data.copy()

            # Apply column mapping if needed (e.g., 'date' -> 'timestamp')
            if "date" in df.columns and "timestamp" not in df.columns:
                df = df.rename(columns={"date": "timestamp"})
                context.log.info(f"[{source_id}] Renamed 'date' column to 'timestamp'")

            # Ensure required columns exist based on database schema
            db_columns = data_source.database["columns"]
            required_columns = list(db_columns.keys())

            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                context.log.warning(
                    f"[{source_id}] Missing columns in data: {missing_columns}"
                )

            # Ensure proper data types
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Process numeric columns with validation
            data_quality = pipeline_settings.get("data_quality", {})

            # Temperature validation and formatting
            if "temperature_2m" in df.columns:
                df["temperature_2m"] = pd.to_numeric(
                    df["temperature_2m"], errors="coerce"
                )
                df["temperature_2m"] = df["temperature_2m"].round(1)

                # Apply temperature range validation
                temp_range = data_quality.get("temperature_range", {})
                if temp_range:
                    min_temp = temp_range.get("min", -50)
                    max_temp = temp_range.get("max", 60)

                    invalid_temp_mask = (df["temperature_2m"] < min_temp) | (
                        df["temperature_2m"] > max_temp
                    )
                    invalid_count = invalid_temp_mask.sum()

                    if invalid_count > 0:
                        context.log.warning(
                            f"[{source_id}] Found {invalid_count} temperature values outside range [{min_temp}, {max_temp}]°C"
                        )
                        df.loc[invalid_temp_mask, "temperature_2m"] = None

            # Humidity validation
            if "relative_humidity_2m" in df.columns:
                df["relative_humidity_2m"] = pd.to_numeric(
                    df["relative_humidity_2m"], errors="coerce"
                )
                df["relative_humidity_2m"] = df["relative_humidity_2m"].round(1)

                humidity_range = data_quality.get("humidity_range", {})
                if humidity_range:
                    min_humidity = humidity_range.get("min", 0)
                    max_humidity = humidity_range.get("max", 100)

                    invalid_humidity_mask = (
                        df["relative_humidity_2m"] < min_humidity
                    ) | (df["relative_humidity_2m"] > max_humidity)
                    invalid_count = invalid_humidity_mask.sum()

                    if invalid_count > 0:
                        context.log.warning(
                            f"[{source_id}] Found {invalid_count} humidity values outside range [{min_humidity}, {max_humidity}]%"
                        )
                        df.loc[invalid_humidity_mask, "relative_humidity_2m"] = None

            # Wind speed validation
            if "wind_speed_10m" in df.columns:
                df["wind_speed_10m"] = pd.to_numeric(
                    df["wind_speed_10m"], errors="coerce"
                )
                df["wind_speed_10m"] = df["wind_speed_10m"].round(1)

                wind_range = data_quality.get("wind_speed_range", {})
                if wind_range:
                    min_wind = wind_range.get("min", 0)
                    max_wind = wind_range.get("max", 200)

                    invalid_wind_mask = (df["wind_speed_10m"] < min_wind) | (
                        df["wind_speed_10m"] > max_wind
                    )
                    invalid_count = invalid_wind_mask.sum()

                    if invalid_count > 0:
                        context.log.warning(
                            f"[{source_id}] Found {invalid_count} wind speed values outside range [{min_wind}, {max_wind}] km/h"
                        )
                        df.loc[invalid_wind_mask, "wind_speed_10m"] = None

            # Handle coordinates
            for coord_col in ["latitude", "longitude"]:
                if coord_col in df.columns:
                    df[coord_col] = pd.to_numeric(df[coord_col], errors="coerce")

            # Check for null values and apply null percentage validation
            initial_count = len(df)
            null_counts = df.isnull().sum()
            total_nulls = null_counts.sum()

            if total_nulls > 0:
                context.log.info(f"[{source_id}] Null value counts:")
                for col, null_count in null_counts.items():
                    if null_count > 0:
                        percentage = (null_count / len(df)) * 100
                        context.log.info(f"  {col}: {null_count} ({percentage:.1f}%)")

            # Apply null percentage threshold
            max_null_percentage = data_quality.get("max_null_percentage", 5)
            for col, null_count in null_counts.items():
                if null_count > 0:
                    percentage = (null_count / len(df)) * 100
                    if percentage > max_null_percentage:
                        context.log.error(
                            f"[{source_id}] Column {col} has {percentage:.1f}% null values, exceeding threshold of {max_null_percentage}%"
                        )
                        raise ValueError(
                            f"Data quality check failed for {source_id}: too many null values in {col}"
                        )

            # Remove rows with null values in critical columns
            critical_columns = ["timestamp"]
            if "temperature_2m" in df.columns:
                critical_columns.append("temperature_2m")

            df = df.dropna(subset=critical_columns)
            final_count = len(df)

            if initial_count != final_count:
                removed_count = initial_count - final_count
                context.log.info(
                    f"[{source_id}] Removed {removed_count} records with null values in critical columns"
                )

            # Sort by timestamp
            if "timestamp" in df.columns:
                df = df.sort_values("timestamp")

            # Final validation
            if df.empty:
                context.log.warning(
                    f"[{source_id}] All data was filtered out during transformation"
                )
            else:
                context.log.info(f"[{source_id}] Transformation completed successfully")
                context.log.info(f"[{source_id}] Final data shape: {df.shape}")

                if "timestamp" in df.columns:
                    context.log.info(
                        f"[{source_id}] First timestamp: {df['timestamp'].iloc[0]}"
                    )
                    context.log.info(
                        f"[{source_id}] Last timestamp: {df['timestamp'].iloc[-1]}"
                    )

                if "temperature_2m" in df.columns:
                    context.log.info(
                        f"[{source_id}] Temperature range: {df['temperature_2m'].min()}°C to {df['temperature_2m'].max()}°C"
                    )

            transformed_data[source_id] = df

        context.log.info(
            f"Successfully transformed data for {len(transformed_data)} sources"
        )
        return transformed_data

    except Exception as e:
        context.log.error(f"Error during bulk data transformation: {str(e)}")
        raise
