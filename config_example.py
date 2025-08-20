import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    BRIGHT_DATA_API_TOKEN = os.getenv("BRIGHT_DATA_API_TOKEN", "your key here")
    BRIGHT_DATA_DATASET_ID = "gd_lwxmeb2u1cniijd7t4"
    OUTPUT_DIR = "output/"
    MAX_CONCURRENT_VALIDATIONS = 3
    VALIDATION_DELAY_MIN = 1.5
    VALIDATION_DELAY_MAX = 3.5
    MAX_SNAPSHOT_WAIT = 600
    
    # NEW: Enhanced Audio sampling configuration
    MIN_SAMPLE_DURATION = 30    # Minimum 30 seconds
    MAX_SAMPLE_DURATION = 3600  # Maximum 1 hour (3600 seconds)
    DEFAULT_SAMPLE_DURATION = 3600  # Default to max duration
    
    # Audio quality settings
    AUDIO_QUALITY_LEVELS = ["192", "128", "96", "64"]  # Quality fallback options
    EXTRACTION_TIMEOUT_BASE = 300  # Base timeout in seconds
