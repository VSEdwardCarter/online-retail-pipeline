from pathlib import Path

PROJECT_PATH = Path(__file__).resolve().parents[1]

RAW_ONLINE = PROJECT_PATH / "data" / "raw"
BRONZE_DIR = PROJECT_PATH / "data" / "bronze"
SILVER_DIR = PROJECT_PATH / "data" / "silver"
GOLD_DIR = PROJECT_PATH / "data" / "gold"

def s(p: Path)->str:
    return str(p)