# subject is where all you items are stored
import os
import time
import shutil
import polars as pl
import duckdb
import pandas as pd
from . import utils
from .item import Item
from . import config

class Subject:
    def __repr__(self):
        return f"Quiver.subject {self.library}/{self.subject}"

    def __init__(self, subject, library):
        self.subject = subject
        self.library = library
        self.items = self.list_items()
        self.snapshots = self.list_snapshots()
        self.inventory = self._create_inventory()
        self.schema = None

        self.subject_path = utils.make_path(self.library, self.subject)
        self.metadata = utils.read_metadata(self.subject_path)

    def _item_path(self, item, as_string=False):
        p = utils.make_path(self.library, self.subject, item)
        if as_string:
            return str(p)
        return p

    def list_items(self,**kwargs):
        dirs = utils.subdirs(utils.make_path(self.library, self.subject))
        if not kwargs:
            return sorted(set(dirs))

        matched = []
        for d in dirs:
            meta = utils.read_metadata(utils.make_path(
                self.library, self.subject, d))
            del meta["_updated"]

            m = 0
            keys = list(meta.keys())
            for k, v in kwargs.items():
                if k in keys and meta[k] == v:
                    m += 1

            if m == len(kwargs):
                matched.append(d)

        return sorted(set(matched))

    def save_subject_metadata(self,metadata):
        """
        Save metadata to the library, should have
        metadata['description'] = "some description of the data in the subject'
        metadata['schema'] = "schema of the data in the subject"
        metadata['source'] = "source of the data in the subject"
        :param metadata:
        :return:
        """
        if utils.path_exists(utils.make_path(self.library, self.subject, 'quiver_metadata.json')):
            existing_metadata = utils.read_metadata(utils.make_path(self.library,self.subject))
            for e in existing_metadata:
                if e not in metadata:
                    metadata[e] = existing_metadata[e]
        utils.write_metadata(self.library, metadata)
        self.metadata = metadata
        return True

    def _create_inventory(self):
        """
        creates a pandas dataframe of all the metadata for the items in the subject
        can be used for advanced sorting and filtering
        :return:
        """
        inventory = []
        for item in self.items:
            metadata = utils.read_metadata(self._item_path(item))

            if item not in metadata.values():
                metadata['item'] = item
            inventory.append(metadata)
        return pd.DataFrame.from_records(inventory)

    def item(self, item, snapshot=None, filters=None, columns=None, sort_on=None):
        return Item(item, self.library, self.subject, snapshot=None, filters=None, columns=None, sort_on=None)

    def index(self, item, index_col='tstamp', last=False):
        i_path = self._item_path(item)
        data = pl.scan_parquet(utils.make_path(i_path, "*.parquet")).select(index_col)
        if last:
            return data.max().collect().item()
        return data.collect()


    def delete_item(self, item, confirm=True):
        if confirm:
            confirm = input(
                f"Are you sure you want to delete {item}? (y/n): ")
            if confirm.lower() != "y":
                print("Deletion aborted")
                return False
        i_path = self._item_path(item)
        shutil.rmtree(i_path)
        self.items = self.list_items()
        return True

    def full_subject(self):
        """
        returns a polars lazy frame of all the items in the subject
        :return:
        """
        if self.schema is None:
            raise ValueError("No schema found. Use `set_schema(item)` to set the golden schema")

        sub_path = utils.make_path(self.library, self.subject)
        sub_gen = utils.make_path(sub_path, "*", "*.parquet")
        return pl.scan_parquet(sub_gen, allow_missing_columns=True, schema=self.schema)

    def set_schema(self, item):
        """
        sets the schema for the subject
        :param item:
        :return:
        """
        i_path = self._item_path(item)
        i_file = list(i_path.glob("*.parquet"))[0]
        schema = pl.read_parquet_schema(i_file)
        schema.pop('__null_dask_index__', None)
        self.schema = schema

    def get_pivot(self, index, column, value):
        """
        creates a pivot table of the subject
        :param index:
        :param column:
        :param value:
        :return:
        """
        df = self.full_subject()
        query_str = f"SELECT {index}, {column}, {value} FROM df;"
        con = duckdb.connect()
        result = con.execute(query_str).df().pivot(index=index, columns=column, values=value)
        return result

    def write(self, item, data_obj, metadata=None, sort_on=None,
              overwrite=False, partition_size=None, include_index=False,
              **kwargs):
        """
        writes item data to a subject within library
        :param item:
        :param data_obj:
        :param metadata:
        :param sort_on: list of columns to sort on
        :param overwrite: bool to overwrite existing item
        :param partition_size: bytes for partition size
        :param kwargs:
        :return:
        """
        i_path = self._item_path(item)
        if utils.path_exists(i_path) and not overwrite:
            raise ValueError("""
                        Item already exists. To overwrite, use `overwrite=True`.
                        Otherwise, use `<subject>.append()`""")

        # convert pandas to polars if needed
        if isinstance(data_obj, pd.DataFrame):
            df = pl.from_pandas(data_obj, include_index=include_index)
        else:
            df = data_obj

        # SORTING
        # sort data for optimal read performance
        if sort_on is not None:
            df = df.sort(sort_on)

        # PARTITIONING
        if partition_size is None:
            partition_size = config.DEFAULT_PARTITION_SIZE
        item_size = utils.get_item_size(self.library, self.subject, item)
        n_partitions = int(1 + item_size // partition_size)
        rows_per_partition = df.height // n_partitions
        df = df.with_columns((pl.arange(0, df.height) // rows_per_partition).alias("partition"))

        # Ensure the last partition includes any remaining rows
        df = df.with_columns(pl.when(pl.col("partition") >= n_partitions).then((n_partitions-1)).otherwise(pl.col("partition")).alias("partition"))

        # Write the DataFrame to Parquet files, partitioned by the 'partition' column
        if overwrite:
            a_path = utils.make_path(i_path, "appended_data.parquet")
            if utils.path_exists(a_path):
                os.remove(a_path)

        df.write_parquet(i_path, partition_by="partition", **kwargs)
        # METADATA
        if metadata is None:
            if utils.path_exists(utils.make_path(i_path, "quiver_metadata.json")):
                metadata = utils.read_metadata(utils.make_path(i_path))
            else:
                metadata = {}
        utils.write_metadata(i_path, metadata)

        if isinstance(self.items, list):
            self.items.append(item)
            self.items = sorted(set(self.items))
        elif isinstance(self.items, set):
            self.items.add(item)

    def lazy_write(self, item, data_obj, metadata=None, sort_on=None, partition_size=None, overwrite=False, **kwargs):
        i_path = self._item_path(item)
        if utils.path_exists(i_path) and not overwrite:
            raise ValueError("""
                                Item already exists. To overwrite, use `overwrite=True`.
                                Otherwise, use `<subject>.append()`""")

        if isinstance(data_obj, pl.LazyFrame):
            df = data_obj
        else:
            raise ValueError("Data object must be a polars LazyFrame")

        if sort_on is not None:
            df = df.sort(sort_on)

        if overwrite:
            a_path = utils.make_path(i_path, "appended_data.parquet")
            if utils.path_exists(a_path):
                os.remove(a_path)
        if partition_size is None:
            partition_size = config.DEFAULT_PARTITION_SIZE

        item_size = utils.get_item_size(self.library, self.subject, item)
        n_partitions = int(1 + item_size // partition_size)
        df_height = df.select(pl.len()).collect().item()
        rows_per_partition = df_height // n_partitions
        df = df.with_columns((pl.arange(0, df_height) // rows_per_partition).alias("partition"))
        df = df.with_columns(pl.when(pl.col("partition") >= n_partitions).then((n_partitions-1)).otherwise(pl.col("partition")).alias("partition"))
        for partition, group in df.group_by("partition"):
            p_path = utils.make_path(self.library, self.subject, item, f"partition={partition}")
            if not utils.path_exists(i_path):
                p_path.mkdir(parents=True)
            # group = group.drop("partition")
            p_i_path = utils.make_path(p_path, f"{item}.parquet")
            df.sink_parquet(p_i_path, **kwargs)
        if metadata is None:
            if utils.path_exists(utils.make_path(i_path, "quiver_metadata.json")):
                metadata = utils.read_metadata(utils.make_path(i_path))
            else:
                metadata = {}
        utils.write_metadata(i_path, metadata)
    def append(self, item, data_obj, sort_on=None, include_index=False, **kwargs):
        """
        appends data to an item in a subject
        first checks if there is a partition on called appended_data.parquet
        if no such file exists then will write to new data to that partition
        if the file exists it will read in the partition, concat the existing appended data with the new data
        and then will overwrite the partition  appended_data.parquet
        :param item:
        :param data_obj:
        :param sort_on:
        :param kwargs:
        :return:
        """
        i_path = self._item_path(item)
        i_files = list(i_path.glob("partition*/*.parquet"))
        i_part = len(i_files)
        a_path = utils.make_path(i_path, "appended_data.parquet")
        data_obj =data_obj.with_columns(pl.lit(i_part).alias('partition'))
        if isinstance(data_obj, pd.DataFrame):
            data_obj = pl.from_pandas(data_obj, include_index=include_index)
        elif isinstance(data_obj, pl.LazyFrame):
            data_obj = data_obj.collect()
        elif isinstance(data_obj, pl.DataFrame):
            pass
        else:
            raise ValueError("Data object must be a pandas or polars DataFrame")

        if utils.path_exists(a_path):
            old_df = pl.read_parquet(a_path)

            append_df = pl.concat([old_df, data_obj])
        else:
            append_df = data_obj
        # SORTING
        append_df = append_df.sort(sort_on)
        # Save the appended data to a new Parquet
        append_df.write_parquet(a_path, **kwargs)
        if append_df.estimated_size() > config.DEFAULT_PARTITION_SIZE:
            print(f"""Warning:{item} Appended data size is larger than default partition size. 
            Consider loading the whole dataset and overwriting partitioning""")

    def create_snapshot(self, snapshot=None):
        if snapshot:
            snapshot = "".join(
                e for e in snapshot if e.isalnum() or e in [".", "_"])
        else:
            snapshot = str(int(time.time() * 1000000))

        src = utils.make_path(self.library, self.subject)
        dst = utils.make_path(src, "_snapshots", snapshot)

        shutil.copytree(src, dst,
                        ignore=shutil.ignore_patterns("_snapshots"))

        self.snapshots = self.list_snapshots()
        return True

    def list_snapshots(self):
        snapshots = utils.subdirs(utils.make_path(
            self.library, self.subject, "_snapshots"))
        return set(snapshots)

    def delete_snapshot(self, snapshot):
        if snapshot not in self.snapshots:
            # raise ValueError("Snapshot `%s` doesn't exist" % snapshot)
            return True

        shutil.rmtree(utils.make_path(self.library, self.subject,
                                      "_snapshots", snapshot))
        self.snapshots = self.list_snapshots()
        return True

    def delete_snapshots(self):
        snapshots_path = utils.make_path(
            self.library, self.subject, "_snapshots")
        shutil.rmtree(snapshots_path)
        os.makedirs(snapshots_path)
        self.snapshots = self.list_snapshots()
        return True