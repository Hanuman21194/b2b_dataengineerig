from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
ENV_PATH = BASE_DIR / ".env"

def load_dotenv_file(path: Path):
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)

load_dotenv_file(ENV_PATH)

DB_CONFIG = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_DB_PORT", "5432")),
    "dbname": os.getenv("SOURCE_DB_NAME", "b2b_source"),
    "user": os.getenv("SOURCE_DB_USER", "9999"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "888888888"),
}

ETL_BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "100000"))
WEBLOG_FILE = DATA_DIR / "b2b_weblogs.log"
MARKETING_LEADS_FILE = DATA_DIR / "marketing_leads.csv"