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
        self.subject_path = utils.make_path(self.library, self.subject)
        self.metadata = utils.read_metadata(self.subject_path)
        self.schema = utils.read_subject_schema(self.subject_path)
        self.partition_on = self.metadata.get('partition_on', None)
        self.sort_on = self.metadata.get('sort_on', None)

    def _item_path(self, item, as_string=False):
        p = utils.make_path(self.library, self.subject, item)
        if as_string:
            return str(p)
        return p

    def list_items(self, sort_items=False, **kwargs):
        """
        gets all the items in the subject and returns a set
        if kwargs are specified, it will filter the items based on the metadata
        if sort_items is True, it will sort the items and return a list in place of a set
        :param sort_items: bool
        :param kwargs: filter items based on metadata
        :return: set of items or list of items if sort_items is True
        """
        dirs = utils.subdirs(utils.make_path(self.library, self.subject))
        if not kwargs:
            if sort_items:
                return sorted(set(dirs))
            return set(dirs)

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

        if sort_items:
            return sorted(set(matched))
        return set(matched)

    def save_subject_metadata(self,metadata, overwrite=False):
        """
        Save metadata to the library, should have
        metadata['description'] = "some description of the data in the subject'
        metadata['schema'] = "schema of the data in the subject"
        metadata['source'] = "source of the data in the subject"
        :param metadata:
        :param overwrite: bool to overwrite existing metadata
        :return:
        """

        utils.write_metadata(self.subject_path, metadata, overwrite=overwrite)
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

    def item(self, item, snapshot=None, filters=None, pre_sort=False, columns=None, sort_on=None):
        return Item(item, self.library, self.subject, snapshot=snapshot, filters=filters,
                    pre_sort=pre_sort, columns=columns, sort_on=sort_on)

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
        if self.subject_path is None:
            self.subject_path = utils.make_path(self.library, self.subject)
        sub_gen = utils.make_path(self.subject_path, "**", "*.parquet")
        return pl.scan_parquet(sub_gen, allow_missing_columns=True, schema=self.schema)

    def set_schema(self, item, save=False):
        """
        sets the schema for the subject based on the schema of a single item
        beware of using this!
        :param item:
        :return:
        """
        i_path = self._item_path(item)
        i_file = list(i_path.glob("*.parquet"))[0]
        schema = pl.read_parquet_schema(i_file, )
        schema.pop('__null_dask_index__', None)
        if save:
            response = input(f"Are you sure you set and for '{self.subject}' base on '{item}'? (y/n): ")
            if response.lower() != 'y':
                print("schema not saved")
            else:
                utils.write_subject_schema(self.subject_path, schema)

        self.schema = schema

    def get_pivot(self, index, column, value):
        """
        creates a pivot table of the subject
        only works where you can pass index/column combo that is unique
        :param index: str
        :param column: str
        :param value: str
        :return: pd.DataFrame
        """
        # Get the path pattern for all Parquet files in the subject
        parquet_pattern = str(utils.make_path(self.library, self.subject, "**", "*.parquet"))

        with duckdb.connect(database=':memory:') as con:
            # Register the parquet files as a virtual table
            con.execute(f"""
                    CREATE OR REPLACE VIEW subject_data AS 
                    SELECT * FROM read_parquet('{parquet_pattern}');
                """)

            # Execute the pivot query directly on the parquet files
            query = f"""
                    PIVOT (
                        SELECT {index}, {column}, {value} 
                        FROM subject_data
                    ) 
                    ON {column}
                    USING first({value})
                    GROUP BY {index};
                """

            # Get the result as a pandas DataFrame
            result = con.execute(query).df()
            # Set the index to the index column
            result = result.set_index(index)
            return result

    def write(self, item, data_obj, metadata=None, sort_on=None, overwrite=False,
              include_index=False, schema=None, **kwargs):
        """
        writes item data to a subject within library
        :param item: str
        :param data_obj: pandas or polars dataframe
        :param metadata: dict
        :param sort_on: list of columns to sort on
        :param overwrite: bool to overwrite existing item
        :param include_index: whether to include index when converting from pandas
        :param schema: optional schema to apply to the data before writing
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

        # Apply schema if it exists
        if schema is None and self.schema:
            schema = self.schema
        
        if schema:
            # Ensure all columns in schema exist in the dataframe
            for col, dtype in schema.items():
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
                else:
                    df = df.with_columns(pl.col(col).cast(dtype))

        # SORTING
        # sort data for optimal read performance
        if sort_on is not None:
            df = df.sort(sort_on)
        elif self.sort_on is not None:
            df = df.sort(self.sort_on)


        if self.partition_on is not None:
            if isinstance(self.partition_on, str):
                raise ValueError("subject.partition_on must be a list.")

            # Verify all partition columns exist in the data
            missing_cols = [col for col in self.partition_on if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Partition columns not found in data: {missing_cols}")

        # Write the DataFrame to Parquet files
        if overwrite and utils.path_exists(i_path):
            shutil.rmtree(i_path)
            i_path.mkdir(parents=True, exist_ok=True)
        else:
            i_path.mkdir(parents=True, exist_ok=True)

        # Write with appropriate partitioning
        if self.partition_on is not None:
            df.write_parquet(i_path, partition_by=self.partition_on, **kwargs)
        else:
            df.write_parquet(i_path / "00000000.parquet", **kwargs)
        
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

    def append(self, item, data_obj, sort_on=None, include_index=False,
               **kwargs):
        """
        Appends data to an item in a subject with optional Hive-style partitioning.

        If partition_on is specified, data will be partitioned according to the specified columns.
        If no partition file exists, new data will be written. If a partition exists, new data
        will be appended to it.

        Args:
            item: Item identifier
            data_obj: Data to append (pandas or polars DataFrame)
            sort_on: Column(s) to sort by before saving
            include_index: Whether to include index when converting from pandas
            **kwargs: Additional arguments passed to write_parquet
        """
        i_path = self._item_path(item)
        if sort_on is None:
            sort_on = self.sort_on

        # Convert input to polars DataFrame if needed
        if isinstance(data_obj, pd.DataFrame):
            data_obj = pl.from_pandas(data_obj, include_index=include_index)
        elif isinstance(data_obj, pl.LazyFrame):
            data_obj = data_obj.collect()
        elif not isinstance(data_obj, pl.DataFrame):
            raise ValueError("Data object must be a pandas DataFrame, polars DataFrame, or polars LazyFrame")

        # Handle Hive-style partitioning
        if self.partition_on is not None:
            # Verify all partition columns exist in the data
            missing_cols = [col for col in self.partition_on if col not in data_obj.columns]
            if missing_cols:
                raise ValueError(f"Partition columns not found in data: {missing_cols}")

            # Group data by partition columns
            for group in data_obj.group_by(self.partition_on):
                partition_values = group[0]  # Tuple of partition values
                partition_data = group[1]    # DataFrame for this partition

                # Create partition directory structure (e.g., "ticker=MSFT/year=2023")
                partition_dir = i_path
                for i, col in enumerate(self.partition_on):
                    partition_dir = partition_dir / f"{col}={partition_values[i]}"

                # Ensure partition directory exists
                partition_dir.mkdir(exist_ok=True, parents=True)

                # Define parquet file path
                parquet_path = partition_dir / "00000000.parquet"

                # Read existing data if it exists, otherwise use empty DataFrame
                if parquet_path.exists():
                    existing_data = pl.read_parquet(parquet_path)
                    combined_data = pl.concat([existing_data, partition_data])
                else:
                    combined_data = partition_data

                # Sort and write the data
                if sort_on:
                    combined_data = combined_data.sort(sort_on)
                elif self.sort_on:
                    combined_data = combined_data.sort(self.sort_on)

                combined_data.write_parquet(parquet_path, **kwargs)

        else:
            # Original non-partitioned behavior
            i_files = list(i_path.glob("**/*.parquet"))
            i_part = len(i_files)
            if i_part > 1:

                a_path = utils.make_path(i_path, "appended_data.parquet")
            else:
                a_path = i_files[0]



            if utils.path_exists(a_path):
                old_df = pl.read_parquet(a_path)
                combined_data = pl.concat([old_df, data_obj])
            else:
                combined_data = data_obj

            # Sort and write the data
            if sort_on is not None:
                combined_data = combined_data.sort(sort_on)


            combined_data.write_parquet(a_path, **kwargs)

            # Warn if data size exceeds partition threshold
            if combined_data.estimated_size() > config.DEFAULT_PARTITION_SIZE:
                print(f"""Warning: {item} Appended data size is larger than default partition size. 
                Consider partitioning data to avoid this warning.""")

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

    def delete_snapshot(self, snapshot, confirm=True):
        """
        Delete a specific snapshot.
        
        Args:
            snapshot: Name of the snapshot to delete
            confirm: If True, prompt for confirmation before deletion. Defaults to True.
        
        Returns:
            bool: True if deletion was successful, False if aborted or snapshot didn't exist
        """
        if snapshot not in self.snapshots:
            return True  # Already doesn't exist
        
        if confirm:
            response = input(f"Are you sure you want to delete snapshot '{snapshot}'? (y/n): ")
            if response.lower() != 'y':
                print("Deletion aborted")
                return False
            
        shutil.rmtree(utils.make_path(self.library, self.subject, "_snapshots", snapshot))
        self.snapshots = self.list_snapshots()
        return True

    def delete_snapshots(self, confirm=True):
        """
        Delete all snapshots for this subject.
        
        Args:
            confirm: If True, prompt for confirmation before deletion. Defaults to True.
        
        Returns:
            bool: True if deletion was successful, False if aborted
        """
        if not self.snapshots:
            return True  # No snapshots to delete
        
        if confirm:
            response = input(f"Are you sure you want to delete ALL {len(self.snapshots)} snapshots? This cannot be undone. (y/n): ")
            if response.lower() != 'y':
                print("Deletion aborted")
                return False
            
        snapshots_path = utils.make_path(self.library, self.subject, "_snapshots")
        shutil.rmtree(snapshots_path)
        os.makedirs(snapshots_path)  # Recreate the empty directory
        self.snapshots = self.list_snapshots()
        return True
