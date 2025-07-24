import polars as pl
from california_filter_refactor import apply_california_filtering

df = pl.read_csv("raw_data.csv")
filtered = apply_california_filtering(df)
filtered.write_csv("cleaned_output.csv")
print(filtered)
