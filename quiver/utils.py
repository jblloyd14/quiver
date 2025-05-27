from datetime import datetime
import json
import shutil
import pandas as pd
import numpy as np
import polars as pl
import duckdb
from pathlib import Path

from . import config


def datetime_to_int64(df, datetime_col):
    """Convert a datetime column in a DataFrame to int64 (nanoseconds since epoch).
    
    Works with both Polars and Pandas DataFrames. The operation is performed in-place
    for Pandas and returns a new DataFrame for Polars.
    
    Args:
        df (pl.DataFrame | pd.DataFrame): Input DataFrame containing the datetime column
        datetime_col (str): Name of the datetime column to convert
        
    Returns:
        pl.DataFrame | pd.DataFrame: DataFrame with the datetime column converted to int64
        
    Raises:
        ValueError: If the specified column is not found or is not a datetime type
    """
    # Handle Polars DataFrame
    if hasattr(df, '_s'):  # Check if it's a Polars DataFrame
        if datetime_col not in df.columns:
            raise ValueError(f"Column '{datetime_col}' not found in DataFrame")
        return df.with_columns(pl.col(datetime_col).dt.epoch('ns').alias(datetime_col))
    
    # Handle Pandas DataFrame
    elif hasattr(df, 'loc'):
        if datetime_col not in df.columns:
            raise ValueError(f"Column '{datetime_col}' not found in DataFrame")
        if not pd.api.types.is_datetime64_any_dtype(df[datetime_col]):
            raise ValueError(f"Column '{datetime_col}' is not a datetime type")
        df[datetime_col] = df[datetime_col].astype('int64')
        return df
    
    else:
        raise ValueError("Input must be either a Polars or Pandas DataFrame")


def subdirs(d):
    """Lists subdirectories in a directory, excluding '_snapshots'."""
    return [o.parts[-1] for o in Path(d).iterdir()
            if o.is_dir() and o.parts[-1] != "_snapshots"]

def get_lib_size(library, pattern="*"):
    """gets the size of a given quiver library"""
    path = get_path(library)
    return sum(f.stat().st_size for f in Path(path).rglob(pattern) if f.is_file())

def get_subject_size(library, subject, pattern="*"):
    """gets the size of a given subjet in a quiver library"""
    path = make_path(library, subject)
    return sum(f.stat().st_size for f in Path(path).rglob(pattern) if f.is_file())

def get_item_size(library, subject, item, pattern="*"):
    """gets the size of given item in a subjet/library"""
    path = make_path(library, subject, item)
    return sum(f.stat().st_size for f in Path(path).rglob(pattern) if f.is_file())

def path_exists(path):
    """checks if a path exists"""
    return path.exists()


def read_metadata(path):
    """reads metadata stored as json in quiver library or subject"""
    dest = make_path(path, "quiver_metadata.json")
    if path_exists(dest):
        with dest.open() as f:
            return json.load(f)
    else:
        return {}


def write_metadata(path, metadata={}):
    """writes metadata to a quiver library or subject

    Args:
        path (str): full path to library or subject
        metadata (dict, optional): metadata to write. Defaults to {}.
    """
    """"""
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
    """
    reads the schema of a quiver subject from a json file and returns it as a polars schema

    parameters
    ----------
    path : str or Path
        full path to the quiver subject

    returns
    -------
    schema : dict
        the polars schema of the subject
    """
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
    """
    construct a path by joining the given arguments together

    Parameters
    ----------
    *args : str or Path
        the components of the path

    Returns
    -------
    Path
        the fully constructed path as a Path object
    """
    # Convert all arguments to Path objects and resolve relative paths
    path_components = [Path(arg) for arg in args if arg is not None]
    if not path_components:
        return Path()
    
    # Join all path components
    result = path_components[0]
    for component in path_components[1:]:
        result = result / component
    
    return result


def get_path(*args):
    """Get the full path by combining the default path with the given arguments.
    
    Args:
        *args: Path components to append to the default path.
        
    Returns:
        Path: The combined path as a Path object.
    """
    base_path = Path(config.DEFAULT_PATH)
    return make_path(base_path, *args) if args else base_path


def set_path(path=None):
    """Set the default storage path for quiver libraries.
    
    Args:
        path (str or Path, optional): The path to set as default. If None, uses the current default.
        
    Returns:
        Path: The absolute path that was set.
        
    Raises:
        ValueError: If a non-local filesystem path is provided.
    """
    if path is None:
        return get_path()
    
    path = Path(str(path).rstrip("/\\ "))
    path_str = str(path)
    
    if "://" in path_str and not path_str.startswith("file://"):
        raise ValueError("PyStore currently only works with local file system")
    
    # Handle file:// URLs
    if path_str.startswith("file://"):
        path = Path(path_str[7:])  # Remove file:// prefix
    
    # Convert to absolute path and resolve any symlinks
    path = path.resolve()
    
    # Update the default path in config
    config.DEFAULT_PATH = path
    return path


def list_libraries():
    """List all available quiver libraries in the default storage location.
    
    Returns:
        list: List of library names (strings)
    """
    lib_path = get_path()
    if not path_exists(lib_path):
        lib_path.mkdir()
    return subdirs(lib_path)


def delete_library(library, confirm=True):
    """Delete a quiver library and all its contents.
    
    Args:
        library (str): Name of the library to delete
        confirm (bool, optional): If True, prompt for confirmation before deletion.
            Defaults to True.
            
    Returns:
        bool: True if deletion was successful, False if aborted
    """
    if confirm:
        response = input(f"Delete library '{library}' and all its contents? (y/n) ")
        if response.lower() != "y":
            print("Deletion aborted")
            return False
    shutil.rmtree(get_path(library))
    return True


def delete_libraries(confirm=True):
    """Delete all quiver libraries and their contents.
    
    Args:
        confirm (bool, optional): If True, prompt for confirmation before deletion.
            Defaults to True.
            
    Returns:
        bool: True if deletion was successful, False if aborted
    """
    if confirm:
        response = input("WARNING: This will delete ALL libraries and data. Continue? (y/n) ")
        if response.lower() != "y":
            print("Deletion aborted")
            return False
    shutil.rmtree(get_path())
    return True

def set_partition_size(size=None):
    """Set the partition size for data storage.
    
    Args:
        size (int, optional): Desired partition size in bytes.
            If None, uses the default partition size from config.
            
    Returns:
        int: The partition size that was set
    """
    if size is None:
        size = config.DEFAULT_PARTITION_SIZE
    config.PARTITION_SIZE = size
    return config.PARTITION_SIZE


def get_partition_size():
    """Get the current partition size setting.
    
    Returns:
        int: The current partition size in bytes
    """
    return config.PARTITION_SIZE