"""
Dagster repository definition for the weather data ETL pipeline.
"""

from dagster import repository

from dagster_project.jobs.weather_job import weather_etl_job


@repository
def weather_data_repository():
    """
    Repository that contains all jobs, assets, schedules, and sensors
    for the weather data ETL pipeline.

    This repository is registered with Dagster and contains:
    - weather_etl_job: Main ETL pipeline job
    """
    return [
        weather_etl_job,
    ]
