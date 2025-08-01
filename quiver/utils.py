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


def write_metadata(path, metadata={}, overwrite=False):
    """writes metadata to a quiver library or subject

    Args:
        path : str or Path
            full path to library or subject
        metadata (dict, optional): metadata to write. Defaults to {}.
        overwrite (bool, optional): if True, overwrite existing metadata. Defaults to False.
    """
    """"""
    now = datetime.now()
    if path_exists(path) and not overwrite:
        existing_meta = read_metadata(path)
        metadata = {**existing_meta, **metadata}
    metadata["_updated"] = now.strftime("%Y-%m-%d %H:%I:%S.%f")
    meta_file = make_path(path, "quiver_metadata.json")
    with meta_file.open("w") as f:
        json.dump(metadata, f, ensure_ascii=False)


def schema_to_dict(schema: dict) -> dict:
    """
    Converts a Polars schema to a serializable dictionary.
    
    Handles all Polars data types including parameterized types like Datetime.
    
    Args:
        schema: Dictionary mapping column names to Polars data types
        
    Returns:
        dict: Dictionary with column names as keys and data type strings as values
    """
    def dtype_to_str(dtype):
        # Handle parameterized types
        if hasattr(dtype, 'time_unit'):
            if hasattr(dtype, 'time_zone') and dtype.time_zone is not None:
                return f"Datetime(time_unit='{dtype.time_unit}', time_zone='{dtype.time_zone}')"
            return f"{dtype.__class__.__name__}(time_unit='{dtype.time_unit}')"
        return str(dtype)
    
    return {col: dtype_to_str(dtype) for col, dtype in schema.items()}

def dict_to_schema(schema_dict: dict) -> dict:
    """
    Converts a dictionary back to a Polars schema.
    
    Args:
        schema_dict: Dictionary mapping column names to data type strings
        
    Returns:
        dict: Dictionary mapping column names to Polars data types
    """
    import re
    
    def str_to_dtype(type_str):
        # Handle parameterized types like Datetime
        match = re.match(r"(\w+)\((.+)\)", type_str)
        if match:
            dtype_name, params_str = match.groups()
            params = {}
            
            # Parse parameters like "time_unit='ns', time_zone='UTC'"
            for param in params_str.split(','):
                param = param.strip()
                if '=' in param:
                    key, value = param.split('=', 1)
                    key = key.strip()
                    # Remove quotes from string values
                    value = value.strip().strip("\'\"")
                    params[key] = value
            
            # Create the appropriate Polars type
            if dtype_name == 'Datetime':
                return getattr(pl, dtype_name)(**params)
            # Add other parameterized types here if needed
            
        # For non-parameterized types
        return getattr(pl, type_str)
    
    return {col: str_to_dtype(dtype_str) for col, dtype_str in schema_dict.items()}

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
            schema_dict = json.load(f)
        return dict_to_schema(schema_dict)
    return {}

def write_subject_schema(path, schema={}):
    """
    writes the schema of a quiver subject to a json file

    parameters
    ----------
    path : str or Path
        full path to the quiver subject
    schema : dict
        the polars schema of the subject
    """
    schema_file = make_path(path, "quiver_schema.json")
    schema_dict = schema_to_dict(schema)
    with schema_file.open("w") as f:
        json.dump(schema_dict, f, ensure_ascii=False)

    print("redeclare subject to use updated schema")


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


def suggest_subject_schema(subject_path, file_pattern="**/*.parquet", sample_fraction=0.1, max_files=100):
    """
    Suggests a Polars schema that would work for all items in a subject.

    This function scans through files in the subject directory matching the file_pattern
    and suggests a schema that can accommodate all the data types found in those files.
    Handles numeric types, datetimes, timestamps, categorical, strings, and booleans.

    Args:
        subject_path (str or Path): Path to the subject directory
        file_pattern (str, optional): Glob pattern for files to scan. Defaults to "**/*.parquet".
        sample_fraction (float, optional): Fraction of files to sample (0-1). Defaults to 0.1.
        max_files (int, optional): Maximum number of files to sample. Defaults to 100.

    Returns:
        dict: A dictionary mapping column names to Polars data types that can accommodate all data.

    Example:
        # Get suggested schema for a subject
        subject = Subject("my_subject", "my_library")
        schema = suggest_subject_schema(subject.subject_path)
    """
    subject_path = Path(subject_path)
    if not subject_path.exists() or not subject_path.is_dir():
        raise ValueError(f"Subject path does not exist or is not a directory: {subject_path}")

    # Find all files matching the pattern in the subject directory
    files = list(subject_path.rglob(file_pattern))
    if not files:
        return {}

    # Determine how many files to sample
    sample_size = min(max(1, int(len(files) * sample_fraction)), max_files, len(files))
    files_to_sample = files[:sample_size]  # Simple sampling from the start

    # Initialize schema with the first file
    try:
        first_schema = pl.scan_parquet(files_to_sample[0]).schema
    except Exception as e:
        raise ValueError(f"Could not read schema from first file {files_to_sample[0]}: {e}")

    suggested_schema = {}

    # For each column, find the most permissive type that fits all values
    for col, dtype in first_schema.items():
        suggested_schema[col] = dtype

    # Function to get the most permissive type between two types
    def get_most_permissive_type(type1, type2):
        if type1 == pl.Null:
            return type2
        if type2 == pl.Null:
            return type1

        if type1 == type2:
            return type1

        if type1 == pl.Boolean or type2 == pl.Boolean:
            if type1 == pl.Boolean and type2 == pl.Boolean:
                return pl.Boolean
            return pl.Utf8

        datetime_types = [pl.Datetime, pl.Datetime('ms'), pl.Datetime('us'), pl.Datetime('ns'),
                         pl.Datetime('ms', '*'), pl.Datetime('us', '*'), pl.Datetime('ns', '*')]

        if type1 in datetime_types or type2 in datetime_types:
            if type1 in datetime_types and type2 in datetime_types:
                precisions = {'ns': 3, 'us': 2, 'ms': 1}
                def get_precision(t):
                    if t == pl.Datetime:
                        return 3
                    return precisions.get(t.time_unit, 3)

                prec1 = get_precision(type1)
                prec2 = get_precision(type2)
                return type1 if prec1 >= prec2 else type2
            return pl.Utf8

        if type1 == pl.Date or type2 == pl.Date:
            if type1 == pl.Date and type2 == pl.Date:
                return pl.Date
            return pl.Utf8

        if type1 == pl.Time or type2 == pl.Time:
            if type1 == pl.Time and type2 == pl.Time:
                return pl.Time
            return pl.Utf8

        if type1 in (pl.Categorical, pl.Utf8) or type2 in (pl.Categorical, pl.Utf8):
            return pl.Utf8

        numeric_types = [
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
            pl.Float32, pl.Float64
        ]

        if type1 in numeric_types and type2 in numeric_types:
            is_signed1 = type1 in [pl.Int8, pl.Int16, pl.Int32, pl.Int64]
            is_signed2 = type2 in [pl.Int8, pl.Int16, pl.Int32, pl.Int64]

            if is_signed1 != is_signed2:
                return pl.Float64

            idx1 = numeric_types.index(type1)
            idx2 = numeric_types.index(type2)
            return numeric_types[max(idx1, idx2)]

        if type1 in numeric_types or type2 in numeric_types:
            return pl.Utf8

        return pl.Utf8

    # Check remaining files to find the most permissive types
    for file_path in files_to_sample[1:]:
        try:
            file_schema = pl.scan_parquet(file_path).schema

            for col, dtype in file_schema.items():
                if col not in suggested_schema:
                    suggested_schema[col] = dtype
                else:
                    suggested_schema[col] = get_most_permissive_type(
                        suggested_schema[col], dtype
                    )
        except Exception as e:
            print(f"Warning: Could not read schema from {file_path}: {e}")

    return suggested_schema