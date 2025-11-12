"""
Configuration loader for the weather ETL pipeline.
Loads and validates configurations from JSON files.
"""

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class DataSourceConfig:
    """Configuration for a data source."""

    id: str
    name: str
    enabled: bool
    api_type: str
    api_config: Dict[str, Any]
    schedule: Dict[str, Any]
    database: Dict[str, Any]
    data_retention: Dict[str, Any]


@dataclass
class PipelineConfig:
    """Configuration for the pipeline."""

    name: str
    description: str
    version: str
    default_timezone: str


@dataclass
class ApiConfig:
    """Configuration for API settings."""

    timeout: int
    retry_attempts: int
    retry_backoff_factor: float
    cache_ttl_seconds: int
    max_concurrent_requests: int
    user_agent: str


@dataclass
class DatabaseConfig:
    """Configuration for database settings."""

    connection_pool_size: int
    connection_timeout: int
    query_timeout: int
    batch_size: int
    default_schema: str


@dataclass
class EnvironmentConfig:
    """Configuration for environment-specific settings."""

    database: Dict[str, Any]
    api: Dict[str, Any]
    logging: Dict[str, Any]


class ConfigLoader:
    """Loads and manages configuration from JSON files."""

    def __init__(self, config_dir: Optional[str] = None):
        """Initialize the config loader.

        Args:
            config_dir: Path to configuration directory. Defaults to ./config
        """
        if config_dir is None:
            # Get the project root directory
            project_root = Path(__file__).parent.parent
            config_dir = project_root / "config"

        self.config_dir = Path(config_dir)
        if not self.config_dir.exists():
            raise ValueError(f"Configuration directory not found: {self.config_dir}")

        self._data_sources: Optional[List[DataSourceConfig]] = None
        self._pipeline_settings: Optional[Dict[str, Any]] = None
        self._environments: Optional[Dict[str, Any]] = None
        self._current_environment: Optional[str] = None

    def _load_json(self, filename: str) -> Dict[str, Any]:
        """Load a JSON file from the config directory."""
        file_path = self.config_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {filename}: {e}")

    def get_data_sources(self, enabled_only: bool = True) -> List[DataSourceConfig]:
        """Get all configured data sources.

        Args:
            enabled_only: If True, return only enabled data sources

        Returns:
            List of DataSourceConfig objects
        """
        if self._data_sources is None:
            config_data = self._load_json("data_sources.json")
            self._data_sources = []

            for source_data in config_data.get("data_sources", []):
                source_config = DataSourceConfig(
                    id=source_data["id"],
                    name=source_data["name"],
                    enabled=source_data["enabled"],
                    api_type=source_data["api_type"],
                    api_config=source_data["api_config"],
                    schedule=source_data["schedule"],
                    database=source_data["database"],
                    data_retention=source_data["data_retention"],
                )
                self._data_sources.append(source_config)

        if enabled_only:
            return [ds for ds in self._data_sources if ds.enabled]
        return self._data_sources

    def get_data_source(self, source_id: str) -> Optional[DataSourceConfig]:
        """Get a specific data source by ID.

        Args:
            source_id: The ID of the data source

        Returns:
            DataSourceConfig if found, None otherwise
        """
        data_sources = self.get_data_sources(enabled_only=False)
        for source in data_sources:
            if source.id == source_id:
                return source
        return None

    def get_pipeline_settings(self) -> Dict[str, Any]:
        """Get pipeline configuration settings."""
        if self._pipeline_settings is None:
            self._pipeline_settings = self._load_json("pipeline_settings.json")
        return self._pipeline_settings

    def get_pipeline_config(self) -> PipelineConfig:
        """Get pipeline configuration as a structured object."""
        settings = self.get_pipeline_settings()
        pipeline_data = settings["pipeline"]

        return PipelineConfig(
            name=pipeline_data["name"],
            description=pipeline_data["description"],
            version=pipeline_data["version"],
            default_timezone=pipeline_data["default_timezone"],
        )

    def get_api_config(self) -> ApiConfig:
        """Get API configuration as a structured object."""
        settings = self.get_pipeline_settings()
        api_data = settings["api"]

        return ApiConfig(
            timeout=api_data["timeout"],
            retry_attempts=api_data["retry_attempts"],
            retry_backoff_factor=api_data["retry_backoff_factor"],
            cache_ttl_seconds=api_data["cache_ttl_seconds"],
            max_concurrent_requests=api_data["max_concurrent_requests"],
            user_agent=api_data["user_agent"],
        )

    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration as a structured object."""
        settings = self.get_pipeline_settings()
        db_data = settings["database"]

        return DatabaseConfig(
            connection_pool_size=db_data["connection_pool_size"],
            connection_timeout=db_data["connection_timeout"],
            query_timeout=db_data["query_timeout"],
            batch_size=db_data["batch_size"],
            default_schema=db_data["default_schema"],
        )

    def get_environments(self) -> Dict[str, Any]:
        """Get environment configurations."""
        if self._environments is None:
            self._environments = self._load_json("environments.json")
        return self._environments

    def get_current_environment(self) -> str:
        """Get the current environment name."""
        if self._current_environment is None:
            env_config = self.get_environments()
            self._current_environment = env_config.get("current_environment", "docker")
        return self._current_environment

    def get_environment_config(
        self, environment: Optional[str] = None
    ) -> EnvironmentConfig:
        """Get configuration for a specific environment.

        Args:
            environment: Environment name. Uses current environment if None.

        Returns:
            EnvironmentConfig object
        """
        if environment is None:
            environment = self.get_current_environment()

        env_configs = self.get_environments()
        if environment not in env_configs["environments"]:
            raise ValueError(f"Environment '{environment}' not found in configuration")

        env_data = env_configs["environments"][environment]

        return EnvironmentConfig(
            database=env_data["database"],
            api=env_data["api"],
            logging=env_data["logging"],
        )

    def get_database_url(self, environment: Optional[str] = None) -> str:
        """Get database connection URL for the specified environment.

        Args:
            environment: Environment name. Uses current environment if None.

        Returns:
            Database connection URL
        """
        env_config = self.get_environment_config(environment)
        db_config = env_config.database

        # Handle environment variable substitution
        host = self._resolve_env_var(db_config["host"])
        port = self._resolve_env_var(str(db_config["port"]))
        database = self._resolve_env_var(db_config["database"])
        username = self._resolve_env_var(db_config["username"])
        password = self._resolve_env_var(db_config["password"])

        return f"postgresql://{username}:{password}@{host}:{port}/{database}"

    def _resolve_env_var(self, value: str) -> str:
        """Resolve environment variables in configuration values.

        Args:
            value: Configuration value that may contain ${VAR_NAME}

        Returns:
            Resolved value
        """
        if not isinstance(value, str):
            return str(value)

        # Simple environment variable substitution
        if value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            return os.getenv(env_var, value)

        return value

    def validate_configuration(self) -> List[str]:
        """Validate all configuration files.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        try:
            # Validate data sources
            data_sources = self.get_data_sources(enabled_only=False)
            if not data_sources:
                errors.append("No data sources configured")

            for source in data_sources:
                if not source.id:
                    errors.append(f"Data source missing ID: {source.name}")
                if not source.api_config:
                    errors.append(f"Data source missing API config: {source.id}")
                if not source.database:
                    errors.append(f"Data source missing database config: {source.id}")

            # Validate pipeline settings
            pipeline_config = self.get_pipeline_config()
            if not pipeline_config.name:
                errors.append("Pipeline name not configured")

            # Validate environment
            env_config = self.get_environment_config()
            if not env_config.database:
                errors.append("Database configuration missing for current environment")

        except Exception as e:
            errors.append(f"Configuration validation error: {str(e)}")

        return errors


# Global config loader instance
_config_loader: Optional[ConfigLoader] = None


def get_config_loader() -> ConfigLoader:
    """Get the global configuration loader instance."""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader()
    return _config_loader


def reload_config():
    """Reload configuration from files."""
    global _config_loader
    _config_loader = ConfigLoader()
