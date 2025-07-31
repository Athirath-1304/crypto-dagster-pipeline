from dagster import Definitions

# Import assets using absolute imports
try:
    from assets import (
        fetch_crypto_data,
        generate_test_crypto_data,
        validate_crypto_data_asset,
        store_validated_crypto_data,
    )
    from schedules import every_15_mins_schedule, test_schedule
except ImportError:
    # Fallback for when running as script
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from assets import (
        fetch_crypto_data,
        generate_test_crypto_data,
        validate_crypto_data_asset,
        store_validated_crypto_data,
    )
    from schedules import every_15_mins_schedule, test_schedule

defs = Definitions(
    assets=[
        fetch_crypto_data,
        generate_test_crypto_data,
        validate_crypto_data_asset,
        store_validated_crypto_data,
    ],
    schedules=[
        every_15_mins_schedule,
        test_schedule,
    ]
)
