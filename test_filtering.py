import polars as pl
import pytest
from california_filter_refactor import apply_california_filtering

def sample_data() -> pl.DataFrame:
    return pl.DataFrame({
        "OWNER_NAME": [
            "ESTATE OF JOHN DOE",  # should be cleaned
            "ACME CORP",           # should be filtered out by keyword
            "JANE DOE",            # should remain
            "SINGLE"               # should be filtered (no space)
        ],
        "CASH_REPORTED": [100.0, 200.0, 50.0, 75.0],
        "OWNER_COUNTRY_CODE": ["US", "USA", None, "CA"],
        "OWNER_STREET_1": [None, "UNKNOWN", "123 Main St", "No Street"],
        "OWNER_STREET_2": ["456 Oak St", "Suite 200", "", ""],
        "OWNER_ZIP": ["90210", "10001", "94103", "75001"],
        "OWNER_STATE": ["CA", "CA", "CA", "CA"],
        "PROPERTY_ID": ["P1", "P2", "P3", "P4"],
        "HOLDER_NAME": ["Holder A", "Holder B", "Holder C", "Holder D"],
        "PROPERTY_TYPE": ["STOCK: AAPL", "MISC", "STOCK: TSLA", "MISC"],
        "CURRENT_CASH_BALANCE": [100.0, 200.0, 0.0, 75.0],
        "SHARES_REPORTED": [10, 0, 5, 1],
        "NO_OF_OWNERS": [None, "1 of 1", "2 of 2", None]
    })

def test_filter_output_shape():
    df = sample_data()
    result = apply_california_filtering(df)
    # Only "JANE DOE" should remain
    assert result.height == 1

def test_column_presence():
    df = sample_data()
    result = apply_california_filtering(df)
    expected_columns = [
        "First_Name", "Last_Name", "OWNER_NAME", "OWNER_STREET_1", "OWNER_CITY",
        "OWNER_ZIP", "OWNER_STATE", "PROPERTY_ID", "HOLDER_NAME", "PROPERTY_TYPE",
        "CASH_REPORTED", "CURRENT_CASH_BALANCE", "SHARES_REPORTED", "NO_OF_OWNERS", "STATE_REPORTED"
    ]
    assert all(col in result.columns for col in expected_columns)

def test_cash_reported_formatting():
    df = sample_data()
    result = apply_california_filtering(df)
    if result.height:
        assert result["CASH_REPORTED"][0].startswith("$")
