"""
Op to load weather data into database using configuration.
"""

import pandas as pd
from dagster import Config, OpExecutionContext, op
from pydantic import Field

from utils.config_loader import get_config_loader
from utils.database import get_database_manager


class LoadWeatherConfig(Config):
    """Configuration for load weather op."""

    source_id: str = Field(description="ID of the data source being loaded")


@op(description="Load transformed weather data into configured database")
def load_weather_to_db(
    context: OpExecutionContext,
    config: LoadWeatherConfig,
    transformed_data: pd.DataFrame,
) -> str:
    """
    Load the transformed weather data into the configured database.

    Args:
        config: Configuration containing source_id
        transformed_data: Transformed weather DataFrame

    Returns:
        Success message with record count
    """
    source_id = config.source_id
    context.log.info(f"Loading weather data to database for source: {source_id}")

    try:
        # Get configuration for this data source
        config_loader = get_config_loader()
        data_source = config_loader.get_data_source(source_id)

        if not data_source:
            raise ValueError(f"Data source configuration not found: {source_id}")

        # Check if we have data to insert
        if transformed_data.empty:
            context.log.warning(
                f"No data to insert for source {source_id} - DataFrame is empty"
            )
            return "No data to insert"

        context.log.info(
            f"Loading {len(transformed_data)} records for {data_source.name}"
        )
        context.log.info(
            f"Target table: {data_source.database['schema']}.{data_source.database['table']}"
        )

        # Get database manager and load data
        db_manager = get_database_manager()
        records_loaded = db_manager.load_data(data_source, transformed_data)

        # Get table information for verification
        table_info = db_manager.get_table_info(data_source)
        context.log.info(
            f"Table now contains {table_info['record_count']} total records"
        )

        if table_info.get("min_date") and table_info.get("max_date"):
            context.log.info(
                f"Data date range in table: {table_info['min_date']} to {table_info['max_date']}"
            )

        success_message = (
            f"Successfully loaded {records_loaded} records for {data_source.name}"
        )
        context.log.info(success_message)

        return success_message

    except Exception as e:
        context.log.error(
            f"Error loading data to database for source {source_id}: {str(e)}"
        )
        raise


@op(description="Load weather data from multiple sources into database")
def load_all_weather_to_db(
    context: OpExecutionContext, transformed_data_dict: dict
) -> dict:
    """
    Load weather data from multiple sources into their respective database tables.

    Args:
        transformed_data_dict: Dictionary mapping source_id to transformed DataFrame

    Returns:
        Dictionary with load results for each source
    """
    context.log.info(f"Loading weather data for {len(transformed_data_dict)} sources")

    if not transformed_data_dict:
        context.log.warning("No data to load")
        return {}

    results = {}
    total_records_loaded = 0

    for source_id, transformed_df in transformed_data_dict.items():
        context.log.info(f"Loading data for source: {source_id}")

        try:
            if transformed_df.empty:
                context.log.warning(f"No data to load for source {source_id}")
                results[source_id] = "No data to load"
                continue

            # Create config for this source
            config = LoadWeatherConfig(source_id=source_id)

            # Load the data
            result = load_weather_to_db.configured(config)(context, transformed_df)

            results[source_id] = result
            total_records_loaded += len(transformed_df)

            context.log.info(f"✓ {source_id}: {len(transformed_df)} records loaded")

        except Exception as e:
            error_msg = f"Failed to load data: {str(e)}"
            results[source_id] = error_msg
            context.log.error(f"✗ {source_id}: {error_msg}")
            # Continue with other sources
            continue

    context.log.info(
        f"Loading complete: {total_records_loaded} total records loaded across all sources"
    )

    return results


@op(description="Clean up old data based on retention policies")
def cleanup_old_weather_data(context: OpExecutionContext) -> dict:
    """
    Clean up old weather data based on configured retention policies.

    Returns:
        Dictionary with cleanup results for each source
    """
    context.log.info("Starting cleanup of old weather data")

    try:
        config_loader = get_config_loader()
        data_sources = config_loader.get_data_sources(
            enabled_only=False
        )  # Check all sources

        if not data_sources:
            context.log.warning("No data sources found for cleanup")
            return {}

        db_manager = get_database_manager()
        results = {}

        for data_source in data_sources:
            retention_config = data_source.data_retention

            if not retention_config.get("cleanup_enabled", True):
                context.log.info(f"Cleanup disabled for source: {data_source.name}")
                results[data_source.id] = "Cleanup disabled"
                continue

            context.log.info(f"Cleaning up data for source: {data_source.name}")

            try:
                db_manager.cleanup_old_data(data_source)
                results[data_source.id] = "Cleanup successful"
                context.log.info(f"✓ Cleanup completed for {data_source.name}")

            except Exception as e:
                error_msg = f"Cleanup failed: {str(e)}"
                results[data_source.id] = error_msg
                context.log.error(f"✗ Cleanup failed for {data_source.name}: {str(e)}")
                continue

        context.log.info("Cleanup process completed")
        return results

    except Exception as e:
        context.log.error(f"Error during cleanup process: {str(e)}")
        raise
