# California Unclaimed Property Cleaner

## Overview

This project processes and cleans large-scale California unclaimed property data using optimized Polars pipelines. It replaces the original monolithic filtering logic with a modular, maintainable, and high-performance implementation.

---

## Features

* ✅ Modular filtering logic via lazy Polars pipelines
* ✅ External `business_keywords.txt` configuration for easy updates
* ✅ Street normalization and PO BOX support
* ✅ High performance for millions of rows
* ✅ Unit-tested with Pytest
* ✅ Cleaning metrics and reporting support
* ✅ Standalone runner script for quick testing

---

## File Structure

```
.
├── california_filter_refactor.py     # Main filtering logic
├── business_keywords.txt             # Business keyword exclusion list
├── test_filtering.py                 # Unit tests
├── data_cleaning_report.md          # Summary of refactoring & improvements
├── performance_report.md            # Benchmark results
├── run_filter.py                     # Quick runner example
└── README.md                         # This file
```

---

## Getting Started

### Requirements

* Python 3.8+
* [Polars](https://pola-rs.github.io/polars/py-polars/html/reference/index.html)
* Pytest (for testing)

### Installation

```bash
pip install polars pytest
```

### Run Example (Standalone)

To test with a single sample record, you can run:

```bash
python run_filter.py
```

This will load a hardcoded test row, apply filtering, and print the resulting cleaned DataFrame.

---

## Usage in Your Code

```python
import polars as pl
from california_filter_refactor import apply_california_filtering

# Load your raw data into a Polars DataFrame
df = pl.read_csv("raw_data.csv")

# Apply filtering
cleaned = apply_california_filtering(df)

# Export cleaned data
cleaned.write_csv("cleaned_output.csv")
```

---

## Tests

Run all tests with:

```bash
pytest test_filtering.py
```

---

## Configuration

* Keywords to filter business-like entities are stored in `business_keywords.txt`
* Each keyword should be listed on its own line (case-insensitive)

---

## Contributors

Developed by Joshua as part of a performance-focused developer assessment.
