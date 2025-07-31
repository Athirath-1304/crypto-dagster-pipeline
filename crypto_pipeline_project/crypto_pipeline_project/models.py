"""
Pydantic models for cryptocurrency data from CoinGecko API.
Based on actual API response structure from fetch_crypto_data() asset.
"""

from typing import Optional, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import re


class CryptoPrice(BaseModel):
    """
    Pydantic model for cryptocurrency price data from CoinGecko API.
    
    This model represents the structure of individual cryptocurrency records
    returned by the CoinGecko /coins/markets endpoint.
    """
    
    # Core identification fields
    id: str = Field(..., description="Unique identifier for the cryptocurrency")
    symbol: str = Field(..., description="Symbol/ticker of the cryptocurrency")
    name: str = Field(..., description="Full name of the cryptocurrency")
    
    # Image URL
    image: str = Field(..., description="URL to the cryptocurrency's logo image")
    
    # Price and market data
    current_price: float = Field(..., description="Current price in USD")
    market_cap: int = Field(..., description="Market capitalization in USD")
    market_cap_rank: int = Field(..., description="Rank by market capitalization")
    fully_diluted_valuation: Optional[int] = Field(None, description="Fully diluted valuation in USD")
    
    # Volume data
    total_volume: int = Field(..., description="Total trading volume in USD (24h)")
    
    # 24h price range
    high_24h: float = Field(..., description="Highest price in the last 24 hours")
    low_24h: float = Field(..., description="Lowest price in the last 24 hours")
    
    # Price changes
    price_change_24h: float = Field(..., description="Price change in USD over 24h")
    price_change_percentage_24h: float = Field(..., description="Price change percentage over 24h")
    
    # Market cap changes
    market_cap_change_24h: float = Field(..., description="Market cap change in USD over 24h")
    market_cap_change_percentage_24h: float = Field(..., description="Market cap change percentage over 24h")
    
    # Supply data
    circulating_supply: float = Field(..., description="Circulating supply")
    total_supply: Optional[float] = Field(None, description="Total supply")
    max_supply: Optional[float] = Field(None, description="Maximum supply")
    
    # All-time high/low data
    ath: float = Field(..., description="All-time high price")
    ath_change_percentage: float = Field(..., description="Percentage change from all-time high")
    ath_date: str = Field(..., description="Date of all-time high")
    atl: float = Field(..., description="All-time low price")
    atl_change_percentage: float = Field(..., description="Percentage change from all-time low")
    atl_date: str = Field(..., description="Date of all-time low")
    
    # ROI data (can be null)
    roi: Optional[Dict[str, Any]] = Field(None, description="Return on investment data")
    
    # Timestamps
    last_updated: str = Field(..., description="Last updated timestamp from API")
    fetched_at: Optional[str] = Field(None, description="Timestamp when data was fetched")
    
    # Validators using Pydantic V2 syntax
    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v):
        """Validate that symbol is lowercase and alphanumeric"""
        if not re.match(r'^[a-z0-9]+$', v):
            raise ValueError('Symbol must be lowercase alphanumeric')
        return v
    
    @field_validator('current_price', 'market_cap', 'total_volume', 'high_24h', 'low_24h')
    @classmethod
    def validate_positive_values(cls, v):
        """Validate that price and volume values are positive"""
        if v < 0:
            raise ValueError('Price and volume values must be positive')
        return v
    
    @field_validator('market_cap_rank')
    @classmethod
    def validate_rank(cls, v):
        """Validate that market cap rank is positive"""
        if v <= 0:
            raise ValueError('Market cap rank must be positive')
        return v
    
    @field_validator('price_change_percentage_24h', 'market_cap_change_percentage_24h')
    @classmethod
    def validate_percentage(cls, v):
        """Validate percentage values are reasonable"""
        if abs(v) > 1000:  # Allow for extreme market movements
            raise ValueError('Percentage change seems unreasonable')
        return v
    
    class Config:
        """Pydantic configuration"""
        # Allow extra fields that might be added by the API
        extra = "ignore"


class CryptoPriceList(BaseModel):
    """
    Model for a list of cryptocurrency prices.
    """
    data: list[CryptoPrice] = Field(..., description="List of cryptocurrency price data")
    count: int = Field(..., description="Number of records in the list")
    fetched_at: datetime = Field(default_factory=datetime.now, description="Timestamp when data was fetched")
    
    @field_validator('count')
    @classmethod
    def validate_count(cls, v, info):
        """Validate that count matches the actual number of records"""
        if 'data' in info.data and v != len(info.data['data']):
            raise ValueError('Count must match the number of records')
        return v
    
    class Config:
        """Pydantic configuration"""
        extra = "ignore"


# Utility functions for working with crypto data
def validate_crypto_data(data: list[dict]) -> list[CryptoPrice]:
    """
    Validate a list of cryptocurrency data dictionaries and convert to CryptoPrice objects.
    
    Args:
        data: List of cryptocurrency data dictionaries from API
        
    Returns:
        List of validated CryptoPrice objects
        
    Raises:
        ValidationError: If data doesn't match the expected schema
    """
    return [CryptoPrice(**record) for record in data]


def create_crypto_price_list(data: list[dict]) -> CryptoPriceList:
    """
    Create a CryptoPriceList from raw API data.
    
    Args:
        data: List of cryptocurrency data dictionaries from API
        
    Returns:
        CryptoPriceList object with validated data
    """
    validated_data = validate_crypto_data(data)
    return CryptoPriceList(
        data=validated_data,
        count=len(validated_data),
        fetched_at=datetime.now()
    )


# Test function
def test_crypto_price_model():
    """Test the CryptoPrice model with real data"""
    try:
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        from assets import fetch_crypto_data
        
        # Fetch real data
        raw_data = fetch_crypto_data()
        
        # Validate first record
        crypto_price = CryptoPrice(**raw_data[0])
        print(f"✅ CryptoPrice model validation passed!")
        print(f"📊 Sample: {crypto_price.name} ({crypto_price.symbol.upper()}) - ${crypto_price.current_price:,.2f}")
        
        # Test the list model
        crypto_list = create_crypto_price_list(raw_data)
        print(f"✅ CryptoPriceList validation passed! {crypto_list.count} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Model validation failed: {str(e)}")
        return False


if __name__ == "__main__":
    test_crypto_price_model() 