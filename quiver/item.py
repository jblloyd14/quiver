import polars as pl
import duckdb

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
        self.sort_on = sort_on

        self._path = utils.make_path(self.library, self.subject, self.item)

        if not self._path.exists():
            raise ValueError(
                f"Item {self.item} does not exist"
                f"Create it by using subject.write({item}, data_obj, ...) in library {self.library}"
            )
        self._files = [f for f in self._path.rglob("*/*.parquet") if f.is_file()]
        self._parquet_path = utils.Path(self._path,"*/*.parquet" )
        if snapshot:
            snap_path = utils.make_path(self.library, self.subject, '_snapshots', self.item)

        self.data = pl.scan_parquet(self._parquet_path).drop('partition')

    def to_pandas(self):
        return self.data.collect().to_pandas()

    def tail(self, n=5):
        return self.data.tail(n).collect().to_pandas()

    def head(self, n=5):
        return self.data.head(n).collect().to_pandas()




