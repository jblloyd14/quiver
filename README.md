heavily based on pystore and arctic
having realized there were many things that were 
inflexible and limiting, I took the framework for pystore
and implement it using polars and duckdb. also added some features
that take advantage of being able to read a directory of parquet files


## examples
```python
import quiver as qs
import polars as pl

# create a quiver library
md = qs.Library("market_data")
metadata = {
    "description": "Market data library",
}
md.save_library_metadata(metadata)
# create subject

subject_metadata = {
    "description": "ETF minute data",
    "source": "interactive brokers",
}
etf_min = md.subject("etf_minute_data", metadata=subject_metadata)

# add data
df = pl.read_parquet("~/data/SPY.parquet")
etf_min.write('SPY', df, metadata={"region": "US", "currency": "USD", "exchange": "NYSE",
              "type": "ETF", "asset_class": "equity", "symbol": "SPY"}
              )    

new_df = fresh data from api
etf_min.append('SPY', new_df)

# read data as polars df
df = etf_min.item('SPY').to_polars()
# or as pandas df
df = etf_min.items('SPY').to_pandas()
# or as polars lazyframe
df = etf_min.items('SPY').data
```
when you load subject.item(), the lazy frame is there under 
the data attribute


subject.full_subject() returns a polars lazyframe with all the items
in the subject


