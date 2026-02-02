import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v and v.strip() else default

def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip() in ("1", "true", "True", "yes", "YES")

@dataclass(frozen=True)
class Settings:
    db_host: str = os.getenv("DB_HOST", "127.0.0.1")
    db_port: int = _int("DB_PORT", 3306)
    db_name: str = os.getenv("DB_NAME", "")
    db_user: str = os.getenv("DB_USER", "")
    db_password: str = os.getenv("DB_PASSWORD", "")

    headless: bool = _bool("HEADLESS", True)
    batch_size: int = _int("BATCH_SIZE", 5)
    check_rows: int = _int("CHECK_ROWS", 50)
    baseline_rows: int = _int("BASELINE_ROWS", 1)
    new_process_window_hours: int = _int("NEW_PROCESS_WINDOW_HOURS", 24)
    interval_minutes: int = _int("INTERVAL_MINUTES", 60)

    dry_run: bool = _bool("DRY_RUN", True)

settings = Settings()
