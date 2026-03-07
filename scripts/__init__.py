# scripts/__init__.py

from .extract import run_extract
from .transform import transform_crypto_data
from .load import load_to_data
from .quality_check import run_quality_check

# Define what is available when someone imports * from src
__all__ = [
    "run_extract",
    "transform_crypto_data", 
    "load_to_data", 
    "run_quality_check"
]
