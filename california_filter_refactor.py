# california_filter_refactor.py

import polars as pl
import re
from typing import Tuple, Set, List

# === Load keywords from config file ===
def load_business_keywords(filepath: str) -> List[str]:
    with open(filepath, 'r', encoding='utf-8') as f:
        return [line.strip().upper() for line in f if line.strip()]

BUSINESS_KEYWORDS = load_business_keywords("business_keywords.txt")
KEYWORD_PATTERN = re.compile(rf"\\b(?:{'|'.join(map(re.escape, BUSINESS_KEYWORDS))})\\b", re.IGNORECASE)
SYMBOL_PATTERN = re.compile(r"[^a-zA-Z]")


# === Modular filtering functions ===
def clean_owner_names(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        pl.col("OWNER_NAME")
        .str.replace_all(r"(?i)\\bESTATE\\s+OF\\b", "")
        .str.strip_chars()
    )


def valid_name_filter(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.filter(pl.col("OWNER_NAME").str.contains(" "))
          .filter(~pl.col("OWNER_NAME").str.contains(KEYWORD_PATTERN.pattern))
    )


def split_owner_name(df: pl.LazyFrame) -> pl.LazyFrame:
    split = pl.col("OWNER_NAME").str.split(" ")
    return df.with_columns([
        split.list.get(i).alias(col)
        for i, col in enumerate([
            "Last_Name", "First_Name", "Middle_Name",
            "AdditionalName_03", "AdditionalName_04", "AdditionalName_05"
        ])
    ])


def name_quality_checks(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(~pl.col("Last_Name").str.contains(SYMBOL_PATTERN.pattern)) \
             .filter(~pl.col("First_Name").str.contains(SYMBOL_PATTERN.pattern)) \
             .filter(pl.col("First_Name").str.len_chars() >= 1)


def clean_country_code(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns([
        pl.when(
            pl.col("OWNER_COUNTRY_CODE").is_null() | (pl.col("OWNER_COUNTRY_CODE").str.strip_chars() == "")
        )
        .then(pl.lit("NAN"))  # ✅ wrap string in pl.lit()
        .otherwise(
            pl.col("OWNER_COUNTRY_CODE").str.to_uppercase().str.strip_chars()
        )
        .alias("OWNER_COUNTRY_CODE")
    ])


def filter_country_code(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(pl.col("OWNER_COUNTRY_CODE").is_in(["US", "USA", "NAN"]))


def normalize_streets(df: pl.LazyFrame) -> pl.LazyFrame:
    street1 = pl.col("OWNER_STREET_1").fill_null("").str.strip_chars()
    street2 = pl.col("OWNER_STREET_2").fill_null("").str.strip_chars()

    df = df.with_columns([
        pl.when(
            (street1.str.to_lowercase().is_in(["unknown", "", "nan"])) &
            (street2.str.contains(r"^\d+\s+\D+"))
        ).then(street2).otherwise(street1).alias("OWNER_STREET_1_NEW")
    ])

    df = df.with_columns([
        pl.when(
            (~pl.col("OWNER_STREET_1_NEW").str.contains(r"^\d+\s+\D+")) &
            (street2.str.contains(r"^\d+\s+\D+"))
        ).then(street2).otherwise(pl.col("OWNER_STREET_1_NEW")).alias("OWNER_STREET_1_FINAL")
    ])

    df = df.with_columns([
        pl.col("OWNER_STREET_1_FINAL")
        .str.replace_all(r"(?i)P\s*\.?\s*O\s*\.?\s*BOX", "PO BOX")
        .alias("OWNER_STREET_1_CLEAN")
    ])

    return df


def filter_street_validity(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(
        pl.col("OWNER_STREET_1_CLEAN").str.contains(r"^\d+\s+\D+") |
        pl.col("OWNER_STREET_1_CLEAN").str.to_uppercase().str.contains("PO BOX")
    ).filter(~pl.col("OWNER_STREET_1_CLEAN").str.to_lowercase().is_in(["unknown", "", "nan"]))


def format_outputs(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns([
        pl.col("OWNER_STREET_1_CLEAN").alias("OWNER_STREET_1"),
        pl.col("OWNER_ZIP").cast(pl.Utf8).str.slice(0, 5).alias("OWNER_ZIP"),
        pl.when(pl.col("NO_OF_OWNERS").is_null()).then(0).otherwise(pl.col("NO_OF_OWNERS")).alias("NO_OF_OWNERS"),
        pl.lit("CALIFORNIA").alias("STATE_REPORTED"),
        pl.col("CASH_REPORTED").round(2).map_elements(lambda x: f"${x:,.2f}", return_dtype=pl.Utf8).alias("CASH_REPORTED"),
        pl.col("CURRENT_CASH_BALANCE").round(2).map_elements(lambda x: f"${x:,.2f}", return_dtype=pl.Utf8).alias("CURRENT_CASH_BALANCE"),
        pl.when(pl.col("PROPERTY_TYPE").str.contains(":"))
          .then(pl.col("PROPERTY_TYPE").str.split(":").list.get(1).str.strip_chars())
          .otherwise(pl.col("PROPERTY_TYPE"))
          .alias("PROPERTY_TYPE")
    ])


def apply_california_filtering(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return df

    print(f"⚡ Applying California filtering to {df.height:,} records...")

    lazy_df = df.lazy()
    lazy_df = lazy_df.filter(pl.col("CASH_REPORTED") >= 0)
    lazy_df = clean_owner_names(lazy_df)
    lazy_df = valid_name_filter(lazy_df)
    lazy_df = split_owner_name(lazy_df)
    lazy_df = name_quality_checks(lazy_df)
    lazy_df = clean_country_code(lazy_df)
    lazy_df = filter_country_code(lazy_df)
    lazy_df = normalize_streets(lazy_df)
    lazy_df = filter_street_validity(lazy_df)
    lazy_df = format_outputs(lazy_df)

    final_df = lazy_df.select([
        "First_Name", "Last_Name", "OWNER_NAME", "OWNER_STREET_1", "OWNER_CITY",
        "OWNER_ZIP", "OWNER_STATE", "PROPERTY_ID", "HOLDER_NAME", "PROPERTY_TYPE",
        "CASH_REPORTED", "CURRENT_CASH_BALANCE", "SHARES_REPORTED", "NO_OF_OWNERS", "STATE_REPORTED"
    ]).collect()

    print(f"✅ California filtering complete: {df.height:,} → {final_df.height:,} records")
    return final_df
