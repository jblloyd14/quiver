from .library import Library
from .utils import (set_path, get_path,
    get_lib_size, get_subject_size, get_item_size,
    write_metadata, read_metadata,
    set_partition_size, get_partition_size,
    list_libraries, delete_libraries, delete_library,
    suggest_subject_schema, write_subject_schema,)

__version__ = "0.1.03"
__author__ = "Some Guy"

__all__ = ["Library", "get_path", "set_path",
           "get_lib_size", "get_subject_size", "get_item_size",
           "write_metadata", "read_metadata",
           "set_partition_size", "get_partition_size",
           "list_libraries", "delete_libraries", "delete_library",
           "suggest_subject_schema", "write_subject_schema"]