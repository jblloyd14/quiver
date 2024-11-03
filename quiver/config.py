import os
from .utils import Path

DEFAULT_PATH = os.environ.get("QUIVER_PATH", Path.home() / "quiver")
DEFAULT_PARTITION_SIZE = 500e+6  # ~500MB
PARTITION_SIZE = 500e+6  # ~500MB