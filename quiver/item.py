import polars as pl

from pathlib import Path

from . import utils

class Item:
    def __repr__(self):
        return f"Quiver.item {self.library}/{self.subject}/{self.item}"

    def __init__(self, item, library, subject, snapshot=None, filters=None, columns=None, sort_on=None):
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
        self.data = self._load_data()
        
    def _load_data(self):
        """Load data with schema validation"""
        data = pl.scan_parquet(self._parquet_path)
        
        # Apply column selection if specified
        if self.columns is not None:
            data = data.select(self.columns)
            
        # Apply sorting if specified
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
        """Validate and enforce schema on the dataframe"""
        if not self.schema:
            return df
            
        # Ensure all columns in schema exist in the dataframe
        for col, dtype in self.schema.items():
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

    def tail(self, n=5):
        return self.data.tail(n).collect().to_pandas()

    def head(self, n=5):
        return self.data.head(n).collect().to_pandas()
