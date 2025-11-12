"""
Configuration loader utility for managing pipeline configurations.
"""

import json
from pathlib import Path
from typing import Any, Dict, List


class ConfigLoader:
    """Handles loading and validation of configuration files."""

    def __init__(self, config_dir: str = None):
        """
        Initialize the configuration loader.

        Args:
            config_dir: Path to the configuration directory. Defaults to relative config/ dir.
        """
        if config_dir is None:
            # Get the project root (two levels up from this file)
            project_root = Path(__file__).parent.parent.parent
            config_dir = project_root / "config"

        self.config_dir = Path(config_dir)
        self._data_sources = None
        self._pipeline_config = None

    def load_data_sources(self) -> Dict[str, Any]:
        """Load data sources configuration from JSON file."""
        if self._data_sources is None:
            config_file = self.config_dir / "data_sources.json"
            if not config_file.exists():
                raise FileNotFoundError(
                    f"Data sources config file not found: {config_file}"
                )

            with open(config_file, "r") as f:
                self._data_sources = json.load(f)

        return self._data_sources

    def load_pipeline_config(self) -> Dict[str, Any]:
        """Load pipeline configuration from JSON file."""
        if self._pipeline_config is None:
            config_file = self.config_dir / "pipeline_config.json"
            if not config_file.exists():
                raise FileNotFoundError(
                    f"Pipeline config file not found: {config_file}"
                )

            with open(config_file, "r") as f:
                self._pipeline_config = json.load(f)

        return self._pipeline_config

    def get_data_source(self, source_key: str) -> Dict[str, Any]:
        """
        Get configuration for a specific data source.

        Args:
            source_key: Key of the data source (e.g., 'openmeteo_berlin')

        Returns:
            Configuration dictionary for the specified data source
        """
        data_sources = self.load_data_sources()

        if source_key not in data_sources["data_sources"]:
            available_sources = list(data_sources["data_sources"].keys())
            raise KeyError(
                f"Data source '{source_key}' not found. Available sources: {available_sources}"
            )

        return data_sources["data_sources"][source_key]

    def get_available_sources(self) -> List[str]:
        """Get list of all available data source keys."""
        data_sources = self.load_data_sources()
        return list(data_sources["data_sources"].keys())

    def validate_source_config(self, source_config: Dict[str, Any]) -> bool:
        """
        Validate that a data source configuration has all required fields.

        Args:
            source_config: Data source configuration dictionary

        Returns:
            True if valid, raises ValueError if invalid
        """
        required_fields = [
            "name",
            "api_url",
            "location",
            "parameters",
            "database",
            "cache_settings",
            "retry_settings",
        ]

        for field in required_fields:
            if field not in source_config:
                raise ValueError(
                    f"Required field '{field}' missing from source configuration"
                )

        # Validate location fields
        location_fields = ["latitude", "longitude", "name"]
        for field in location_fields:
            if field not in source_config["location"]:
                raise ValueError(f"Required location field '{field}' missing")

        # Validate database fields
        database_fields = ["table_name", "schema"]
        for field in database_fields:
            if field not in source_config["database"]:
                raise ValueError(f"Required database field '{field}' missing")

        return True

    def get_pipeline_setting(self, setting_path: str, default: Any = None) -> Any:
        """
        Get a specific pipeline setting using dot notation.

        Args:
            setting_path: Path to setting using dot notation (e.g., 'database.batch_size')
            default: Default value if setting not found

        Returns:
            Setting value or default
        """
        config = self.load_pipeline_config()

        keys = setting_path.split(".")
        value = config

        try:
            for key in keys:
                value = value[key]
            return value
        except KeyError:
            if default is not None:
                return default
            raise KeyError(f"Pipeline setting '{setting_path}' not found")


# Global config loader instance
_config_loader = None


def get_config_loader() -> ConfigLoader:
    """Get the global configuration loader instance."""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader()
    return _config_loader


def get_data_source_config(source_key: str) -> Dict[str, Any]:
    """Convenience function to get a data source configuration."""
    return get_config_loader().get_data_source(source_key)


def get_pipeline_config() -> Dict[str, Any]:
    """Convenience function to get the pipeline configuration."""
    return get_config_loader().load_pipeline_config()


def get_available_data_sources() -> List[str]:
    """Convenience function to get available data sources."""
    return get_config_loader().get_available_sources()
