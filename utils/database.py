"""
Database utilities for managing connections and operations.
"""

import logging

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Index,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base

from utils.config_loader import DataSourceConfig, get_config_loader

# Load environment variables
load_dotenv()

Base = declarative_base()
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations."""

    def __init__(self, environment: str = None):
        """Initialize database manager.

        Args:
            environment: Environment to use for configuration
        """
        self.config_loader = get_config_loader()
        self.environment = environment
        self._engine = None
        self.db_config = self.config_loader.get_database_config()

    def get_engine(self):
        """Get or create database engine."""
        if self._engine is None:
            database_url = self.config_loader.get_database_url(self.environment)
            self._engine = create_engine(
                database_url,
                pool_size=self.db_config.connection_pool_size,
                pool_timeout=self.db_config.connection_timeout,
                echo=False,  # Set to True for SQL debug logging
            )
            logger.info(
                f"Created database engine for environment: {self.config_loader.get_current_environment()}"
            )

        return self._engine

    def create_table_for_source(self, data_source: DataSourceConfig):
        """Create table for a data source if it doesn't exist.

        Args:
            data_source: Data source configuration
        """
        engine = self.get_engine()
        metadata = MetaData()

        table_name = data_source.database["table"]
        schema_name = data_source.database.get("schema", self.db_config.default_schema)
        columns_config = data_source.database["columns"]

        logger.info(f"Creating/verifying table: {schema_name}.{table_name}")

        # Create schema if specified
        if schema_name and schema_name != "public":
            with engine.connect() as connection:
                try:
                    connection.execute(
                        text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                    )
                    connection.commit()
                    logger.info(f"Schema created/verified: {schema_name}")
                except SQLAlchemyError as e:
                    logger.warning(f"Could not create schema {schema_name}: {e}")

        # Build columns
        columns = []
        for col_name, col_type in columns_config.items():
            if col_type.upper() == "TIMESTAMP":
                columns.append(Column(col_name, DateTime))
            elif col_type.upper() == "FLOAT":
                columns.append(Column(col_name, Float))
            elif col_type.upper().startswith("VARCHAR"):
                # Extract length from VARCHAR(n)
                if "(" in col_type:
                    length = int(col_type.split("(")[1].split(")")[0])
                    columns.append(Column(col_name, String(length)))
                else:
                    columns.append(Column(col_name, String))
            else:
                logger.warning(
                    f"Unknown column type {col_type} for {col_name}, defaulting to String"
                )
                columns.append(Column(col_name, String))

        # Create table object
        table = Table(
            table_name,
            metadata,
            *columns,
            schema=schema_name if schema_name != "public" else None,
        )

        # Create indexes if specified
        indexes_config = data_source.database.get("indexes", [])
        for index_col in indexes_config:
            if index_col in columns_config:
                index_name = f"ix_{table_name}_{index_col}"
                Index(index_name, table.c[index_col])

        # Create table
        try:
            metadata.create_all(engine)
            logger.info(f"Table created/verified: {schema_name}.{table_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error creating table {schema_name}.{table_name}: {e}")
            raise

        return table

    def load_data(self, data_source: DataSourceConfig, df: pd.DataFrame) -> int:
        """Load data into the database for a data source.

        Args:
            data_source: Data source configuration
            df: DataFrame to load

        Returns:
            Number of records loaded
        """
        if df.empty:
            logger.warning(f"No data to load for {data_source.id}")
            return 0

        engine = self.get_engine()
        table_name = data_source.database["table"]
        schema_name = data_source.database.get("schema", self.db_config.default_schema)

        # Create full table name with schema
        full_table_name = (
            f"{schema_name}.{table_name}" if schema_name != "public" else table_name
        )

        logger.info(f"Loading {len(df)} records into {full_table_name}")

        try:
            # Create table if it doesn't exist
            self.create_table_for_source(data_source)

            # Handle duplicates by deleting existing data for the same time range
            self._handle_duplicates(data_source, df, engine)

            # Load data using pandas to_sql
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema_name if schema_name != "public" else None,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=self.db_config.batch_size,
            )

            logger.info(f"Successfully loaded {len(df)} records into {full_table_name}")

            # Log sample of data
            self._log_data_sample(data_source, engine)

            return len(df)

        except Exception as e:
            logger.error(f"Error loading data into {full_table_name}: {str(e)}")
            raise

    def _handle_duplicates(
        self, data_source: DataSourceConfig, df: pd.DataFrame, engine
    ):
        """Handle duplicate records by removing existing data for the same time range.

        Args:
            data_source: Data source configuration
            df: DataFrame being loaded
            engine: Database engine
        """
        if df.empty or "timestamp" not in df.columns:
            return

        table_name = data_source.database["table"]
        schema_name = data_source.database.get("schema", self.db_config.default_schema)
        full_table_name = (
            f"{schema_name}.{table_name}" if schema_name != "public" else table_name
        )

        min_date = df["timestamp"].min()
        max_date = df["timestamp"].max()

        logger.info(f"Removing existing data for date range: {min_date} to {max_date}")

        # Build delete query based on primary key
        primary_key = data_source.database.get("primary_key", ["timestamp"])

        if "location_name" in primary_key and "location_name" in df.columns:
            # Include location in delete condition
            location_name = df["location_name"].iloc[0]
            delete_query = text(f"""
                DELETE FROM {full_table_name} 
                WHERE timestamp >= :min_date 
                AND timestamp <= :max_date 
                AND location_name = :location_name
            """)
            params = {
                "min_date": min_date,
                "max_date": max_date,
                "location_name": location_name,
            }
        else:
            # Just use timestamp
            delete_query = text(f"""
                DELETE FROM {full_table_name}
                WHERE timestamp >= :min_date 
                AND timestamp <= :max_date
            """)
            params = {"min_date": min_date, "max_date": max_date}

        try:
            with engine.connect() as connection:
                result = connection.execute(delete_query, params)
                connection.commit()

                if result.rowcount > 0:
                    logger.info(
                        f"Deleted {result.rowcount} existing records from {full_table_name}"
                    )

        except SQLAlchemyError as e:
            logger.error(f"Error deleting existing records: {e}")
            # Continue with load anyway

    def _log_data_sample(self, data_source: DataSourceConfig, engine):
        """Log a sample of the loaded data.

        Args:
            data_source: Data source configuration
            engine: Database engine
        """
        table_name = data_source.database["table"]
        schema_name = data_source.database.get("schema", self.db_config.default_schema)
        full_table_name = (
            f"{schema_name}.{table_name}" if schema_name != "public" else table_name
        )

        try:
            with engine.connect() as connection:
                # Get total count
                count_query = text(f"SELECT COUNT(*) as count FROM {full_table_name}")
                result = connection.execute(count_query)
                total_records = result.fetchone()[0]
                logger.info(f"Total records in {full_table_name}: {total_records}")

                # Show recent sample
                sample_query = text(f"""
                    SELECT * FROM {full_table_name} 
                    ORDER BY timestamp DESC 
                    LIMIT 3
                """)
                sample_result = connection.execute(sample_query)

                logger.info(f"Sample data from {full_table_name}:")
                for row in sample_result:
                    row_dict = dict(row._mapping)
                    timestamp = row_dict.get("timestamp", "N/A")
                    location = row_dict.get("location_name", "N/A")
                    temp = row_dict.get("temperature_2m", "N/A")
                    logger.info(f"  {timestamp} | {location} | {temp}°C")

        except SQLAlchemyError as e:
            logger.warning(f"Could not retrieve sample data: {e}")

    def cleanup_old_data(self, data_source: DataSourceConfig):
        """Clean up old data based on retention policy.

        Args:
            data_source: Data source configuration
        """
        retention_config = data_source.data_retention
        if not retention_config.get("cleanup_enabled", True):
            return

        retention_days = retention_config.get("days", 365)
        table_name = data_source.database["table"]
        schema_name = data_source.database.get("schema", self.db_config.default_schema)
        full_table_name = (
            f"{schema_name}.{table_name}" if schema_name != "public" else table_name
        )

        logger.info(
            f"Cleaning up data older than {retention_days} days from {full_table_name}"
        )

        engine = self.get_engine()

        try:
            with engine.connect() as connection:
                cleanup_query = text(f"""
                    DELETE FROM {full_table_name}
                    WHERE timestamp < NOW() - INTERVAL '{retention_days} days'
                """)
                result = connection.execute(cleanup_query)
                connection.commit()

                if result.rowcount > 0:
                    logger.info(
                        f"Cleaned up {result.rowcount} old records from {full_table_name}"
                    )
                else:
                    logger.info(f"No old records to clean up from {full_table_name}")

        except SQLAlchemyError as e:
            logger.error(f"Error during cleanup of {full_table_name}: {e}")

    def get_table_info(self, data_source: DataSourceConfig) -> dict:
        """Get information about a table.

        Args:
            data_source: Data source configuration

        Returns:
            Dictionary with table information
        """
        engine = self.get_engine()
        table_name = data_source.database["table"]
        schema_name = data_source.database.get("schema", self.db_config.default_schema)
        full_table_name = (
            f"{schema_name}.{table_name}" if schema_name != "public" else table_name
        )

        try:
            with engine.connect() as connection:
                # Get record count
                count_query = text(f"SELECT COUNT(*) as count FROM {full_table_name}")
                count_result = connection.execute(count_query)
                record_count = count_result.fetchone()[0]

                # Get date range
                date_query = text(f"""
                    SELECT 
                        MIN(timestamp) as min_date,
                        MAX(timestamp) as max_date
                    FROM {full_table_name}
                    WHERE timestamp IS NOT NULL
                """)
                date_result = connection.execute(date_query)
                date_row = date_result.fetchone()

                return {
                    "table_name": full_table_name,
                    "record_count": record_count,
                    "min_date": date_row[0] if date_row else None,
                    "max_date": date_row[1] if date_row else None,
                }

        except SQLAlchemyError as e:
            logger.error(f"Error getting table info for {full_table_name}: {e}")
            return {
                "table_name": full_table_name,
                "record_count": 0,
                "min_date": None,
                "max_date": None,
                "error": str(e),
            }

    def dispose(self):
        """Dispose of database connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("Database engine disposed")


# Global database manager instance
_db_manager = None


def get_database_manager(environment: str = None) -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager(environment)
    return _db_manager
