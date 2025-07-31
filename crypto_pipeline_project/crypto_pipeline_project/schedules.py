"""
Schedules for the crypto pipeline.
"""

from dagster import ScheduleDefinition, define_asset_job

# Import assets using absolute import
try:
    from assets import fetch_crypto_data, validate_crypto_data_asset, store_validated_crypto_data
except ImportError:
    # Fallback for when running as script
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from assets import fetch_crypto_data, validate_crypto_data_asset, store_validated_crypto_data

# Define a job that includes the full pipeline
crypto_pipeline_job = define_asset_job(
    name="crypto_pipeline_job",
    selection=[fetch_crypto_data, validate_crypto_data_asset, store_validated_crypto_data],
    description="Full crypto data pipeline: fetch → validate → store"
)

# Schedule that runs every 15 minutes
every_15_mins_schedule = ScheduleDefinition(
    job=crypto_pipeline_job,
    cron_schedule="*/15 * * * *",
    name="crypto_pipeline_15min_schedule",
    description="Runs the crypto pipeline every 15 minutes to fetch, validate, and store crypto data"
)

# Alternative schedule for testing (every 5 minutes)
test_schedule = ScheduleDefinition(
    job=crypto_pipeline_job,
    cron_schedule="*/5 * * * *",
    name="crypto_pipeline_test_schedule",
    description="Test schedule that runs every 5 minutes for development"
) 