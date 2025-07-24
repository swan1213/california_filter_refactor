# California Data Cleaning Report

## Overview

This report summarizes the optimizations, refactoring, and improvements made to the `apply_california_filtering()` function, which processes California unclaimed property data using Polars.

---

## Key Improvements

### 1. **Performance Optimization**

* Replaced the original 17-step lazy chain with modular filtering functions.
* Removed unnecessary intermediate columns.
* Reduced memory usage by streamlining DataFrame transformations.

### 2. **Maintainability Enhancements**

* Refactored each major step into separate reusable functions.
* Made business keyword logic configurable and dynamic via `business_keywords.txt`.
* Simplified complex regular expressions and redundant logic.

### 3. **Street Address Normalization**

* Introduced reusable logic to choose between `OWNER_STREET_1` and `OWNER_STREET_2`.
* Improved PO BOX handling and validation.
* Filtered out unusable or empty addresses with a cleaner approach.

### 4. **Keyword Matching System**

* Business entity terms (e.g., "BANK", "ESTATE", etc.) are now externally loaded.
* Uses case-insensitive regex pattern compiled only once.
* Allows non-developers to update business rules without code changes.

### 5. **Output & Formatting**

* Normalized and rounded numeric outputs like `CASH_REPORTED`.
* Extracted stock symbols from `PROPERTY_TYPE` field.
* Added fixed metadata fields (e.g., `STATE_REPORTED`).

---

## Data Quality Metrics

The following metrics can be logged per batch:

| Step                       | Description                    | Example Count |
| -------------------------- | ------------------------------ | ------------- |
| Initial Records            | Total input rows               | 1,000,000     |
| After Negative Cash Filter | Remove rows with negative cash | 980,000       |
| After Name Validation      | Keep valid full names          | 820,000       |
| After Keyword Filter       | Remove business entities       | 740,000       |
| After Address Validation   | Keep valid street info         | 690,000       |
| Final Output               | Final record count             | 685,000       |

---

## Usage Instructions

* Place your keywords in `business_keywords.txt`, one per line.
* Import and call `apply_california_filtering(df)` with a Polars DataFrame.
* The function returns a cleaned Polars DataFrame ready for export.

---

## Testing

* A test file `test_filtering.py` is provided with Pytest-based validations.
* Includes column checks, output size verification, and formatting assertions.

---

## Suggested Extensions

* Add a logger to collect real-time metrics for each filter.
* Cache regex results for very large datasets.
* Export final report (CSV/Markdown) per batch run.

---

## Author

Refactored by Joshua as part of a software developer assessment.
Date: July 2025
