from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

UPLOAD_TMP_DIR = BASE_DIR / "data" / "uploads" / "tmp"