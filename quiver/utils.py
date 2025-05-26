import os
from datetime import datetime
import json
import shutil
import pandas as pd
import numpy as np
import polars as pl
import duckdb
from pathlib import Path


from . import config


def read_csv(urlpath, *args, **kwargs):
    def rename_dask_index(df, name):
        df.index.name = name
        return df

    index_col = index_name = None

    if "index" in kwargs:
        del kwargs["index"]
    if "index_col" in kwargs:
        index_col = kwargs["index_col"]
        if isinstance(index_col, list):
            index_col = index_col[0]
        del kwargs["index_col"]
    if "index_name" in kwargs:
        index_name = kwargs["index_name"]
        del kwargs["index_name"]

    df = pl.read_csv(urlpath, *args, **kwargs)

    if index_col is not None:
        df = df.set_index(index_col)

    if index_name is not None:
        df = df.map_partitions(rename_dask_index, index_name)

    return df


def datetime_to_int64(df):
    """ convert datetime index to epoch int
    allows for cross language/platform portability
    """

    if isinstance(df.index, dd.Index) and (
            isinstance(df.index, pd.DatetimeIndex) and
            any(df.index.nanosecond) > 0):
        df.index = df.index.astype(np.int64)  # / 1e9

    return df


def subdirs(d):
    """ use this to construct paths for future storage support """
    return [o.parts[-1] for o in Path(d).iterdir()
            if o.is_dir() and o.parts[-1] != "_snapshots"]

def get_lib_size(library, pattern="*"):
    """ use this to construct paths for future storage support """
    path = get_path(library)
    return sum(f.stat().st_size for f in Path(path).rglob(pattern) if f.is_file())

def get_subject_size(library, subject, pattern="*"):
    """ use this to construct paths for future storage support """
    path = make_path(library, subject)
    return sum(f.stat().st_size for f in Path(path).rglob(pattern) if f.is_file())

def get_item_size(library, subject, item, pattern="*"):
    """ use this to construct paths for future storage support """
    path = make_path(library, subject, item)
    return sum(f.stat().st_size for f in Path(path).rglob(pattern) if f.is_file())

def path_exists(path):
    """ use this to construct paths for future storage support """
    return path.exists()


def read_metadata(path):
    """ use this to construct paths for future storage support """
    dest = make_path(path, "quiver_metadata.json")
    if path_exists(dest):
        with dest.open() as f:
            return json.load(f)
    else:
        return {}


def write_metadata(path, metadata={}):
    """ use this to construct paths for future storage support """
    now = datetime.now()
    metadata["_updated"] = now.strftime("%Y-%m-%d %H:%I:%S.%f")
    meta_file = make_path(path, "quiver_metadata.json")
    with meta_file.open("w") as f:
        json.dump(metadata, f, ensure_ascii=False)


def schema_to_json(schema: dict):
    """
    converts a polars schema to a json schema
    """
    schema_as_str = {col:str(dtype) for col,dtype in schema.items()}
    return json.dumps(schema_as_str)

def json_to_schema(schema_json: str):
    """
    converts a json schema to a polars schema
    """
    schema_as_str = json.loads(schema_json)
    return {col: getattr(pl, dtype) for col,dtype in schema_as_str.items()}

def read_subject_schema(path):
    schema_path = make_path(path, "quiver_schema.json")
    if path_exists(schema_path):
        with schema_path.open() as f:
            schema_json = json.load(f)
            return json_to_schema(schema_json)
    else:
        return {}

def write_subject_schema(path, schema={}):
    """
    writes the schema of a quiver subject to a json file

    parameters
    ----------
    path : str
        full path to the quiver subject
    schema : dict
        the polars schema of the subject
    """
    """ use this to construct paths for future storage support """
    schema_file = make_path(path, "quiver_schema.json")
    with schema_file.open("w") as f:
        json.dump(schema, f, ensure_ascii=False)


def make_path(*args):
    """ use this to construct paths for future storage support """
    # return Path(os.path.join(*args))
    return Path(*args)


def get_path(*args):
    """ use this to construct paths for future storage support """
    # return Path(os.path.join(config.DEFAULT_PATH, *args))
    return Path(config.DEFAULT_PATH, *args)


def set_path(path):
    if path is None:
        path = get_path()

    else:
        path = path.rstrip("/").rstrip("\\").rstrip(" ")
        if "://" in path and "file://" not in path:
            raise ValueError(
                "PyStore currently only works with local file system")

    config.DEFAULT_PATH = path
    path = get_path()

    # if path does not exist - create it
    if not path_exists(get_path()):
        os.makedirs(get_path())

    return get_path()


def list_libraries():
    if not path_exists(get_path()):
        os.makedirs(get_path())
    return subdirs(get_path())


def delete_library(library, confirm=True):
    if confirm:
        confirm = input(f"Delete store {library}? (y/n)")
        if confirm.lower() != "y":
            print("Deletion aborted")
            return False
    shutil.rmtree(get_path(library))
    return True


def delete_libraries(confirm=True):
    if confirm:
        confirm = input(f"This will delete all libraries and data do you want to continue? (y/n)")
        if confirm.lower() != "y":
            print("Deletion aborted")
            return False
    shutil.rmtree(get_path())
    return True

def set_partition_size(size=None):
    if size is None:
        size = config.DEFAULT_PARTITION_SIZE * 1
    config.PARTITION_SIZE = size
    return config.PARTITION_SIZE


def get_partition_size():
    return config.PARTITION_SIZE