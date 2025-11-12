"""
Simple Dagster repository for weather data ETL pipeline.
"""

from dagster import repository

from dagster_project.jobs.weather_job import weather_etl_job


@repository
def weather_data_repository():
    """
    Simple repository containing the weather ETL job.
    """
    return [weather_etl_job]
