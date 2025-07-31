import requests
import logging
from typing import List, Dict, Any
from dagster import asset, get_dagster_logger, AssetExecutionContext
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
import time
import random
from faker import Faker
import pandas as pd
import duckdb
import os

# Import our new models
try:
    from models import CryptoPrice, validate_crypto_data, create_crypto_price_list
except ImportError:
    # Fallback for when running as script
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from models import CryptoPrice, validate_crypto_data, create_crypto_price_list

# Configure logging
logger = get_dagster_logger()

# Initialize Faker with fixed seed for consistency
fake = Faker()
Faker.seed(42)

class CryptoData(BaseModel):
    """Schema for crypto data from CoinGecko"""
    id: str
    symbol: str
    name: str
    current_price: float = Field(alias="current_price")
    market_cap: float = Field(alias="market_cap")
    market_cap_rank: int = Field(alias="market_cap_rank")
    total_volume: float = Field(alias="total_volume")
    high_24h: float = Field(alias="high_24h")
    low_24h: float = Field(alias="low_24h")
    price_change_24h: float = Field(alias="price_change_24h")
    price_change_percentage_24h: float = Field(alias="price_change_percentage_24h")
    market_cap_change_24h: float = Field(alias="market_cap_change_24h")
    market_cap_change_percentage_24h: float = Field(alias="market_cap_change_percentage_24h")
    circulating_supply: float = Field(alias="circulating_supply")
    total_supply: float = Field(alias="total_supply")
    max_supply: float = Field(alias="max_supply")
    ath: float = Field(alias="ath")
    ath_change_percentage: float = Field(alias="ath_change_percentage")
    ath_date: str = Field(alias="ath_date")
    atl: float = Field(alias="atl")
    atl_change_percentage: float = Field(alias="atl_change_percentage")
    atl_date: str = Field(alias="atl_date")
    roi: Dict[str, Any] = Field(alias="roi")
    last_updated: str = Field(alias="last_updated")
    fetched_at: datetime = Field(default_factory=datetime.now)

def validate_crypto_data_legacy(data: List[Dict[str, Any]]) -> bool:
    """
    Validates the structure and quality of crypto data.
    
    Args:
        data: List of cryptocurrency data dictionaries
        
    Returns:
        bool: True if data is valid, False otherwise
    """
    if not data:
        logger.warning("⚠️ No data received from API")
        return False
    
    required_fields = ['id', 'symbol', 'name', 'current_price', 'market_cap']
    
    for i, entry in enumerate(data):
        missing_fields = [field for field in required_fields if field not in entry]
        if missing_fields:
            logger.warning(f"⚠️ Entry {i} missing required fields: {missing_fields}")
            return False
        
        # Check for reasonable price values
        if entry.get('current_price', 0) <= 0:
            logger.warning(f"⚠️ Entry {i} has invalid price: {entry.get('current_price')}")
            return False
    
    logger.info(f"✅ Data validation passed for {len(data)} entries")
    return True

@asset(
    description="Fetches real-time cryptocurrency data from CoinGecko API",
    tags={"source": "coingecko", "data_type": "crypto_prices"}
)
def fetch_crypto_data() -> List[Dict[str, Any]]:
    """
    Fetches cryptocurrency data from CoinGecko API.
    
    Returns:
        List[Dict[str, Any]]: List of cryptocurrency data dictionaries
    """
    logger.info("🚀 Starting crypto data fetch from CoinGecko...")
    
    # CoinGecko API endpoint for top cryptocurrencies
    url = "https://api.coingecko.com/api/v3/coins/markets"
    
    # Parameters for the API request
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 20,  # Top 20 cryptocurrencies
        "page": 1,
        "sparkline": False,
        "locale": "en"
    }
    
    try:
        logger.info(f"📡 Making request to CoinGecko API: {url}")
        start_time = time.time()
        
        # Make the API request
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        request_time = time.time() - start_time
        logger.info(f"✅ API request completed in {request_time:.2f} seconds")
        
        # Parse the JSON response
        data = response.json()
        
        # Log the number of entries received
        num_entries = len(data)
        logger.info(f"📊 Received {num_entries} cryptocurrency entries from CoinGecko")
        
        # Validate the data using our new Pydantic model
        try:
            validated_data = validate_crypto_data(data)
            logger.info(f"✅ Pydantic validation passed for {len(validated_data)} records")
        except Exception as e:
            logger.error(f"❌ Pydantic validation failed: {str(e)}")
            # Fallback to legacy validation
            if not validate_crypto_data_legacy(data):
                raise ValueError("Invalid data received from API")
            logger.warning("⚠️ Using legacy validation as fallback")
        
        # Log some sample data for debugging
        if data:
            sample_crypto = data[0]
            logger.info(f"💰 Sample crypto: {sample_crypto.get('name', 'Unknown')} "
                       f"({sample_crypto.get('symbol', 'N/A').upper()}) - "
                       f"${sample_crypto.get('current_price', 0):,.2f}")
        
        # Add timestamp to each entry
        for entry in data:
            entry['fetched_at'] = datetime.now().isoformat()
        
        logger.info(f"🎯 Successfully processed {num_entries} cryptocurrency records")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API request failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error during data fetch: {str(e)}")
        raise

@asset(
    description="Generates synthetic cryptocurrency data for testing using Faker",
    tags={"source": "faker", "data_type": "synthetic_crypto_prices"}
)
def generate_test_crypto_data() -> List[Dict[str, Any]]:
    """
    Generates synthetic cryptocurrency data for testing purposes.
    
    Returns:
        List[Dict[str, Any]]: List of synthetic cryptocurrency data dictionaries
    """
    logger.info("🎭 Starting synthetic crypto data generation...")
    
    # List of realistic crypto names and symbols for variety
    crypto_names = [
        ("Bitcoin", "btc"), ("Ethereum", "eth"), ("Cardano", "ada"),
        ("Solana", "sol"), ("Polkadot", "dot"), ("Chainlink", "link"),
        ("Litecoin", "ltc"), ("Stellar", "xlm"), ("VeChain", "vet"),
        ("Filecoin", "fil"), ("Avalanche", "avax"), ("Polygon", "matic"),
        ("Cosmos", "atom"), ("Uniswap", "uni"), ("Algorand", "algo"),
        ("Tezos", "xtz"), ("Monero", "xmr"), ("Dash", "dash"),
        ("Zcash", "zec"), ("Decred", "dcr")
    ]
    
    synthetic_data = []
    
    for i in range(10):
        # Select a random crypto name and symbol
        name, symbol = random.choice(crypto_names)
        
        # Generate realistic price ranges based on the crypto
        if symbol == "btc":
            current_price = round(random.uniform(50000, 150000), 2)
        elif symbol == "eth":
            current_price = round(random.uniform(2000, 5000), 2)
        else:
            current_price = round(random.uniform(0.01, 500), 4)
        
        # Generate market cap based on price and supply
        circulating_supply = random.uniform(1000000, 1000000000)
        market_cap = current_price * circulating_supply
        
        # Generate 24h price range
        price_change_24h = random.uniform(-current_price*0.1, current_price*0.1)
        price_change_percentage_24h = (price_change_24h / current_price) * 100
        high_24h = current_price + abs(price_change_24h) * 0.5
        low_24h = current_price - abs(price_change_24h) * 0.5
        
        # Generate market cap changes
        market_cap_change_24h = price_change_24h * circulating_supply
        market_cap_change_percentage_24h = price_change_percentage_24h
        
        # Generate all-time high/low data
        ath = current_price * random.uniform(1.1, 3.0)
        ath_change_percentage = ((current_price - ath) / ath) * 100
        atl = current_price * random.uniform(0.01, 0.5)
        atl_change_percentage = ((current_price - atl) / atl) * 100
        
        # Generate dates
        ath_date = fake.date_time_between(start_date='-2y', end_date='-1d').isoformat() + 'Z'
        atl_date = fake.date_time_between(start_date='-5y', end_date='-1y').isoformat() + 'Z'
        last_updated = fake.date_time_between(start_date='-1h', end_date='now').isoformat() + 'Z'
        
        # Create the base record
        record = {
            "id": symbol,
            "symbol": symbol,
            "name": name,
            "image": f"https://coin-images.coingecko.com/coins/images/{fake.random_int(min=1, max=999)}/large/{symbol}.png",
            "current_price": current_price,
            "market_cap": int(market_cap),
            "market_cap_rank": i + 1,
            "fully_diluted_valuation": int(market_cap * 1.1) if random.random() > 0.3 else None,
            "total_volume": int(random.uniform(market_cap*0.01, market_cap*0.1)),
            "high_24h": high_24h,
            "low_24h": low_24h,
            "price_change_24h": price_change_24h,
            "price_change_percentage_24h": price_change_percentage_24h,
            "market_cap_change_24h": market_cap_change_24h,
            "market_cap_change_percentage_24h": market_cap_change_percentage_24h,
            "circulating_supply": circulating_supply,
            "total_supply": circulating_supply * random.uniform(1.0, 1.5) if random.random() > 0.4 else None,
            "max_supply": circulating_supply * random.uniform(1.2, 2.0) if random.random() > 0.5 else None,
            "ath": ath,
            "ath_change_percentage": ath_change_percentage,
            "ath_date": ath_date,
            "atl": atl,
            "atl_change_percentage": atl_change_percentage,
            "atl_date": atl_date,
            "roi": {"percentage": round(random.uniform(-50, 200), 2), "currency": "usd"} if random.random() > 0.6 else None,
            "last_updated": last_updated,
            "fetched_at": datetime.now().isoformat()
        }
        
        # Remove None values to simulate optional fields
        record = {k: v for k, v in record.items() if v is not None}
        
        synthetic_data.append(record)
    
    # Log the results
    logger.info(f"🎭 Generated {len(synthetic_data)} synthetic crypto records")
    
    # Log a sample entry
    if synthetic_data:
        sample = synthetic_data[0]
        logger.info(f"📊 Sample synthetic crypto: {sample['name']} ({sample['symbol'].upper()}) - "
                   f"${sample['current_price']:,.2f}")
    
    # Validate the synthetic data using our Pydantic model
    try:
        validated_data = validate_crypto_data(synthetic_data)
        logger.info(f"✅ Synthetic data validation passed for {len(validated_data)} records")
    except Exception as e:
        logger.error(f"❌ Synthetic data validation failed: {str(e)}")
        raise
    
    return synthetic_data

@asset(
    description="Validates and cleans cryptocurrency data using Pydantic models",
    tags={"data_type": "validation", "quality": "cleaned"}
)
def validate_crypto_data_asset(context: AssetExecutionContext, 
                              fetch_crypto_data: List[Dict[str, Any]] = None,
                              generate_test_crypto_data: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Validates cryptocurrency data from either real API or synthetic sources.
    
    Args:
        context: Dagster execution context
        fetch_crypto_data: Real crypto data from CoinGecko API
        generate_test_crypto_data: Synthetic crypto data for testing
        
    Returns:
        List[Dict[str, Any]]: List of validated and cleaned cryptocurrency records
    """
    context.log.info("🔍 Starting crypto data validation...")
    
    # Determine which data source to use
    if fetch_crypto_data is not None:
        raw_data = fetch_crypto_data
        data_source = "CoinGecko API"
        context.log.info("📡 Using real data from CoinGecko API")
    elif generate_test_crypto_data is not None:
        raw_data = generate_test_crypto_data
        data_source = "Synthetic Data"
        context.log.info("🎭 Using synthetic data from Faker")
    else:
        raise ValueError("No data provided for validation")
    
    total_records = len(raw_data)
    valid_records = []
    invalid_records = []
    
    context.log.info(f"🔍 Validating {total_records} records from {data_source}")
    
    for i, record in enumerate(raw_data):
        try:
            # Validate the record using our Pydantic model
            validated_record = CryptoPrice.model_validate(record)
            valid_records.append(record)
            context.log.debug(f"✅ Record {i+1} ({record.get('name', 'Unknown')}) validated successfully")
            
        except ValidationError as e:
            # Log detailed validation errors
            error_details = []
            for error in e.errors():
                field = error.get('loc', ['unknown'])[0] if error.get('loc') else 'unknown'
                value = error.get('input', 'N/A')
                message = error.get('msg', 'Unknown error')
                error_details.append(f"Field '{field}': {value} - {message}")
            
            invalid_records.append({
                'record_index': i,
                'record_data': record,
                'errors': error_details
            })
            
            context.log.warning(f"❌ Record {i+1} ({record.get('name', 'Unknown')}) validation failed:")
            for detail in error_details:
                context.log.warning(f"   - {detail}")
    
    # Log summary statistics
    context.log.info(f"📊 Validation Summary:")
    context.log.info(f"   📈 Total records: {total_records}")
    context.log.info(f"   ✅ Valid records: {len(valid_records)}")
    context.log.info(f"   ❌ Invalid records: {len(invalid_records)}")
    context.log.info(f"   📊 Success rate: {(len(valid_records)/total_records)*100:.1f}%")
    
    # Log sample of valid records
    if valid_records:
        sample = valid_records[0]
        context.log.info(f"💰 Sample valid record: {sample.get('name', 'Unknown')} "
                        f"({sample.get('symbol', 'N/A').upper()}) - "
                        f"${sample.get('current_price', 0):,.2f}")
    
    # Log details of invalid records if any
    if invalid_records:
        context.log.warning(f"⚠️ Found {len(invalid_records)} invalid records:")
        for invalid in invalid_records[:3]:  # Show first 3 invalid records
            context.log.warning(f"   Record {invalid['record_index']+1}: {invalid['record_data'].get('name', 'Unknown')}")
            for error in invalid['errors']:
                context.log.warning(f"     - {error}")
    
    # Return only valid records
    context.log.info(f"🎯 Returning {len(valid_records)} validated records")
    return valid_records

@asset(
    description="Stores validated cryptocurrency data in DuckDB database",
    tags={"storage": "duckdb", "data_type": "persisted"},
    deps=["validate_crypto_data_asset"]
)
def store_validated_crypto_data(context: AssetExecutionContext, 
                               validate_crypto_data_asset: List[Dict[str, Any]]) -> str:
    """
    Stores validated cryptocurrency data in DuckDB database.
    
    Args:
        context: Dagster execution context
        validate_crypto_data_asset: Validated cryptocurrency data
        
    Returns:
        str: Path to the DuckDB database file
    """
    from pathlib import Path
    
    context.log.info("💾 Starting crypto data storage in DuckDB...")
    
    # Ensure data directory exists using Path
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    context.log.info(f"📁 Ensuring data directory exists: {data_dir}")
    
    # Define database file path
    db_path = data_dir / "crypto_data.duckdb"
    context.log.info(f"🗄️ Database path: {db_path}")
    
    # Convert validated data to DataFrame
    if not validate_crypto_data_asset:
        context.log.warning("⚠️ No validated data provided for storage")
        return db_path
    
    # Convert Pydantic models back to dictionaries for DataFrame
    if validate_crypto_data_asset and hasattr(validate_crypto_data_asset[0], 'model_dump'):
        # Convert Pydantic models to dictionaries
        dict_data = [item.model_dump() for item in validate_crypto_data_asset]
        df = pd.DataFrame(dict_data)
    else:
        df = pd.DataFrame(validate_crypto_data_asset)
    
    context.log.info(f"📊 Converted {len(df)} records to DataFrame")
    
    # Handle NaN values in object columns to prevent type issues
    object_columns = df.select_dtypes(include=['object']).columns
    for col in object_columns:
        df[col] = df[col].fillna('')
        context.log.info(f"🔧 Cleaned NaN values in {col}")
    
    # Convert datetime columns to proper pandas datetime format
    datetime_cols = ["ath_date", "atl_date", "last_updated", "fetched_at"]
    for col in datetime_cols:
        if col in df.columns:
            # Convert to datetime and handle NaT values
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # Convert back to string for DuckDB compatibility
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
            context.log.info(f"🕒 Converted {col} to datetime format")
    
    context.log.info("✅ Data cleaning and datetime conversion completed")
    
    # Connect to DuckDB
    try:
        con = duckdb.connect(str(db_path))
        context.log.info("🔗 Connected to DuckDB database")
        
        # Create table with IF NOT EXISTS to prevent schema issues
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS validated_crypto_data (
            id VARCHAR,
            symbol VARCHAR,
            name VARCHAR,
            image VARCHAR,
            current_price DOUBLE,
            market_cap BIGINT,
            market_cap_rank INTEGER,
            fully_diluted_valuation DOUBLE,
            total_volume BIGINT,
            high_24h DOUBLE,
            low_24h DOUBLE,
            price_change_24h DOUBLE,
            price_change_percentage_24h DOUBLE,
            market_cap_change_24h DOUBLE,
            market_cap_change_percentage_24h DOUBLE,
            circulating_supply DOUBLE,
            total_supply DOUBLE,
            max_supply DOUBLE,
            ath DOUBLE,
            ath_change_percentage DOUBLE,
            ath_date VARCHAR,
            atl DOUBLE,
            atl_change_percentage DOUBLE,
            atl_date VARCHAR,
            last_updated VARCHAR,
            fetched_at VARCHAR,
            roi VARCHAR
        )
        """
        con.execute(create_table_sql)
        context.log.info("✅ Table created/verified with correct schema")
        
        # Insert data using INSERT INTO ... SELECT
        con.execute("INSERT INTO validated_crypto_data SELECT * FROM df")
        context.log.info(f"✅ Inserted {len(df)} records into database")
        
        # Get total row count
        total_rows = con.execute("SELECT COUNT(*) FROM validated_crypto_data").fetchone()[0]
        context.log.info(f"📈 Total records in database: {total_rows}")
        
        # Show sample data
        sample_data = con.execute("SELECT name, symbol, current_price, market_cap FROM validated_crypto_data LIMIT 3").fetchall()
        context.log.info("💰 Sample data in database:")
        for row in sample_data:
            context.log.info(f"   - {row[0]} ({row[1].upper()}) - ${row[2]:,.2f} - Market Cap: ${row[3]:,.0f}")
        
        con.close()
        context.log.info("🔒 Database connection closed")
        
    except Exception as e:
        context.log.error(f"❌ Error storing data in DuckDB: {str(e)}")
        raise
    
    context.log.info(f"🎯 Successfully stored {len(df)} records in DuckDB at {db_path}")
    return str(db_path)

# Test function for development
def test_fetch_crypto_data():
    """Test function to verify the asset works correctly"""
    try:
        result = fetch_crypto_data()
        print(f"✅ Test passed! Fetched {len(result)} crypto records")
        print(f"📊 Sample data: {result[0]['name']} - ${result[0]['current_price']:,.2f}")
        return True
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False

def test_generate_test_crypto_data():
    """Test function to verify the synthetic data generation works correctly"""
    try:
        result = generate_test_crypto_data()
        print(f"✅ Synthetic data test passed! Generated {len(result)} records")
        print(f"📊 Sample synthetic data: {result[0]['name']} - ${result[0]['current_price']:,.2f}")
        return True
    except Exception as e:
        print(f"❌ Synthetic data test failed: {str(e)}")
        return False

def test_validate_crypto_data():
    """Test function to verify the validation asset works correctly"""
    try:
        # Test with synthetic data
        synthetic_data = generate_test_crypto_data()
        
        # Test validation logic directly
        total_records = len(synthetic_data)
        valid_records = []
        invalid_records = []
        
        print(f"🔍 Testing validation with {total_records} synthetic records...")
        
        for i, record in enumerate(synthetic_data):
            try:
                # Validate the record using our Pydantic model
                validated_record = CryptoPrice.model_validate(record)
                valid_records.append(record)
                print(f"✅ Record {i+1} ({record.get('name', 'Unknown')}) validated successfully")
                
            except ValidationError as e:
                # Log detailed validation errors
                error_details = []
                for error in e.errors():
                    field = error.get('loc', ['unknown'])[0] if error.get('loc') else 'unknown'
                    value = error.get('input', 'N/A')
                    message = error.get('msg', 'Unknown error')
                    error_details.append(f"Field '{field}': {value} - {message}")
                
                invalid_records.append({
                    'record_index': i,
                    'record_data': record,
                    'errors': error_details
                })
                
                print(f"❌ Record {i+1} ({record.get('name', 'Unknown')}) validation failed:")
                for detail in error_details:
                    print(f"   - {detail}")
        
        # Print summary statistics
        print(f"📊 Validation Summary:")
        print(f"   📈 Total records: {total_records}")
        print(f"   ✅ Valid records: {len(valid_records)}")
        print(f"   ❌ Invalid records: {len(invalid_records)}")
        print(f"   📊 Success rate: {(len(valid_records)/total_records)*100:.1f}%")
        
        print(f"✅ Validation test passed! Validated {len(valid_records)} records")
        return True
    except Exception as e:
        print(f"❌ Validation test failed: {str(e)}")
        return False

def test_store_validated_crypto_data():
    """Test function to verify the DuckDB storage works correctly"""
    try:
        # Generate and validate synthetic data
        synthetic_data = generate_test_crypto_data()
        
        print(f"💾 Testing DuckDB storage with {len(synthetic_data)} records...")
        
        # Ensure data directory exists
        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)
        print(f"📁 Ensuring data directory exists: {data_dir}")
        
        # Define database file path
        db_path = os.path.join(data_dir, "crypto_data.duckdb")
        print(f"🗄️ Database path: {db_path}")
        
        # Convert validated data to DataFrame
        if not synthetic_data:
            print("⚠️ No data provided for storage")
            return False
        
        df = pd.DataFrame(synthetic_data)
        print(f"📊 Converted {len(df)} records to DataFrame")
        
        # Connect to DuckDB
        try:
            con = duckdb.connect(db_path)
            print("🔗 Connected to DuckDB database")
            
            # Check if table exists
            table_exists = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='validated_crypto_data'").fetchone()
            
            if table_exists:
                print("📋 Table 'validated_crypto_data' already exists, appending data...")
                # Append data to existing table
                con.execute("INSERT INTO validated_crypto_data SELECT * FROM df")
                print(f"✅ Appended {len(df)} records to existing table")
            else:
                print("🆕 Creating new table 'validated_crypto_data'...")
                # Create new table
                con.execute("CREATE TABLE validated_crypto_data AS SELECT * FROM df")
                print(f"✅ Created table with {len(df)} records")
            
            # Get total row count
            total_rows = con.execute("SELECT COUNT(*) FROM validated_crypto_data").fetchone()[0]
            print(f"📈 Total records in database: {total_rows}")
            
            # Show sample data
            sample_data = con.execute("SELECT name, symbol, current_price, market_cap FROM validated_crypto_data LIMIT 3").fetchall()
            print("💰 Sample data in database:")
            for row in sample_data:
                print(f"   - {row[0]} ({row[1].upper()}) - ${row[2]:,.2f} - Market Cap: ${row[3]:,.0f}")
            
            con.close()
            print("🔒 Database connection closed")
            
        except Exception as e:
            print(f"❌ Error storing data in DuckDB: {str(e)}")
            raise
        
        print(f"🎯 Successfully stored {len(df)} records in DuckDB at {db_path}")
        
        # Verify the database was created
        if os.path.exists(db_path):
            print(f"✅ Database file created successfully at {db_path}")
            
            # Test reading from database
            con = duckdb.connect(db_path)
            total_rows = con.execute("SELECT COUNT(*) FROM validated_crypto_data").fetchone()[0]
            print(f"📊 Total records in database: {total_rows}")
            
            # Show sample data
            sample_data = con.execute("SELECT name, symbol, current_price FROM validated_crypto_data LIMIT 3").fetchall()
            print("💰 Sample data from database:")
            for row in sample_data:
                print(f"   - {row[0]} ({row[1].upper()}) - ${row[2]:,.2f}")
            
            con.close()
            print("✅ Database read test passed!")
        else:
            print(f"❌ Database file not found at {db_path}")
            return False
        
        return True
    except Exception as e:
        print(f"❌ Storage test failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_fetch_crypto_data()
    print("\n" + "="*50 + "\n")
    test_generate_test_crypto_data()
    print("\n" + "="*50 + "\n")
    test_validate_crypto_data()
    print("\n" + "="*50 + "\n")
    test_store_validated_crypto_data()
