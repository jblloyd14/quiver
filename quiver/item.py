import polars as pl
import duckdb
from pathlib import Path

from . import utils

class Item:
    def __repr__(self):
        return f"Quiver.item {self.library}/{self.subject}/{self.item}"

    def __init__(self, item, library, subject, snapshot=None, filters=None, pre_sort=False,
                 columns=None, sort_on=None):
        self.item = item
        self.library = library
        self.subject = subject
        self.snapshot = snapshot
        self.filters = filters
        self.columns = columns

        self._subject_path = utils.make_path(self.library, self.subject)

        # Handle snapshot path if specified
        if self.snapshot:
            self._path = utils.make_path(self.library, self.subject, '_snapshots', self.item, self.snapshot)
            if not self._path.exists():
                raise ValueError(f"Snapshot {self.snapshot} for item {self.item} does not exist")
            self._parquet_path = str(self._path / "**/*.parquet")
        else:
            self._path = utils.make_path(self.library, self.subject, self.item)
            if not self._path.exists():
                raise ValueError(
                    f"Item {self.item} does not exist. "
                    f"Create it by using subject.write({item}, data_obj, ...) in library {self.library}"
                )
            self._files = [f for f in self._path.rglob("*.parquet") if f.is_file()]
            self._parquet_path = str(Path(self._path, "**/*.parquet"))
        self.subject_metadata = utils.read_metadata(self._subject_path)
        if sort_on is None:
            self.sort_on = self.subject_metadata.get('sort_on', None)
        else:
            self.sort_on = sort_on
        self.metadata = utils.read_metadata(self._path)
        self.schema = utils.read_subject_schema(self._subject_path)

        # Load the data
        self.data = self._load_data(pre_sort=pre_sort)
        
    def _load_data(self, pre_sort=False):
        """Load data with schema validation"""
        data = pl.scan_parquet(self._parquet_path)
        
        # Apply column selection if specified
        if self.columns is not None:
            data = data.select(self.columns)


            
        # Apply sorting if specified
        if pre_sort:
            if self.sort_on is not None:
                if isinstance(self.sort_on, str):
                    sort_columns = [self.sort_on]
                else:
                    sort_columns = list(self.sort_on)
                data = data.sort(by=sort_columns)
        
        # Apply filters if specified
        if self.filters is not None:
            data = data.filter(self.filters)
            
        return data

    def _validate_schema(self, df):
        """Validate and enforce schema on the dataframe, only for columns that were selected"""
        if not self.schema:
            return df
            
        # If columns were specified, only validate those columns
        columns_to_validate = self.columns if self.columns is not None else self.schema.keys()
        
        # Ensure all columns in schema exist in the dataframe
        for col in columns_to_validate:
            if col not in self.schema:
                continue
                
            dtype = self.schema[col]
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
            else:
                df = df.with_columns(pl.col(col).cast(dtype))
        return df

    def to_pandas(self):
        df = self.data.collect()
        df = self._validate_schema(df)
        return df.to_pandas()

    def to_polars(self):
        df = self.data.collect()
        df = self._validate_schema(df)
        return df

    def tail(self, n=5, as_pandas=False):
        if as_pandas:
            return self.data.tail(n).collect().to_pandas()
        return self.data.tail(n).collect()

    def head(self, n=5, as_pandas=False):
        return self.data.head(n).collect().to_pandas()
        
    def query_data(self, sql, params=None, as_pandas=True):
        """
        Execute a SQL query against the item's parquet files using DuckDB.
        
        Args:
            sql: SQL query string. You can reference the data using a table named 'item_data'.
                 Example: "SELECT * FROM item_data WHERE column = ?"
            params: Optional dictionary of parameters to use with the query.
                   Parameters can be referenced in the SQL using $param_name syntax.
            as_pandas: If True (default), returns a pandas DataFrame. 
                      If False, returns a polars DataFrame.
                   
        Returns:
            pandas.DataFrame or polars.DataFrame: The query results.
            
        Examples:
            # Basic query (returns pandas DataFrame by default)
            df = item.query_duckdb("SELECT * FROM item_data WHERE price > 100")
            
            # Get results as polars DataFrame
            df = item.query_duckdb(
                "SELECT * FROM item_data WHERE price > 100",
                as_pandas=False
            )
            
            # With parameters
            df = item.query_duckdb(
                "SELECT * FROM item_data WHERE date >= $start_date AND date <= $end_date",
                params={"start_date": "2023-01-01", "end_date": "2023-12-31"}
            )
            
            # Aggregation
            df = item.query_duckdb('''
                SELECT category, AVG(price) as avg_price, COUNT(*) as count 
                FROM item_data 
                GROUP BY category
            ''')
        """
        with duckdb.connect(database=':memory:') as con:
            # Register the parquet files as a view
            # DuckDB will automatically handle Hive-style partitioning
            con.execute(f"""
                CREATE OR REPLACE VIEW item_data AS 
                SELECT * FROM read_parquet('{self._parquet_path}', hive_partitioning=1);
            """)
            
            # If params are provided, use them in the query
            if params:
                # Register parameters
                for param_name, param_value in params.items():
                    con.execute(f"SET {param_name} = ?", (param_value,))
                
                # Execute the query with parameters
                result = con.execute(sql)
            else:
                # Execute the query without parameters
                result = con.execute(sql)
            
            # Return as requested type
            if as_pandas:
                return result.df()
            return result.pl()
    
    def explain_query(self, sql):
        """
        Get the query execution plan for a DuckDB query without executing it.
        
        Args:
            sql: SQL query to explain
            
        Returns:
            str: The query execution plan
        """
        con = duckdb.connect(database=':memory:')
        try:
            # Register the parquet files as a view
            con.execute(f"""
                CREATE OR REPLACE VIEW item_data AS 
                SELECT * FROM read_parquet('{self._parquet_path}', hive_partitioning=1);
            """)
            
            # Get the query plan
            plan = con.execute(f"EXPLAIN {sql}").fetchall()
            return "\n".join(str(row[0]) for row in plan)
            
        finally:
            con.close()
