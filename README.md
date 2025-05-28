from prompt_toolkit.layout.utils import explode_text_fragmentsheavily based on pystore and arctic
having realized there were many things that were 
inflexible and limiting, I took the framework for pystore
and implement it using polars and duckdb. also added some features
that take advantage of being able to read a directory of parquet files

to install
```bash
  pip install git+https://github.com/jblloyd14/quiver.git --upgrade --no-cache-dir
```

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
```

## create subject

### when creating a subject you should specify how the data is partitioned else it by default quiver will create a column called 'partition' and partition on that
#### hack to have only single partition is make a column called 'partition' and set it to a single value like 'all' or 0
```python
subject_metadata = {
    "description": "ETF minute data",
    "source": "interactive brokers",
}

etf_min = md.subject("etf_minute_data", partition_on='partition',metadata=subject_metadata)

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

## Dealing with schema issues
when loading the full subject or appending data, you can sometimes have issues with
with the schema not being consistent in polars so you should use the utils function
```python

suggested_schema = utils.suggest_subject_schema(etf_min.subject_path, sample_fraction=0.1, max_files=100)
utils.write_subject_schema(etf_min.subject_path, suggesteds_chema)

```
