import json
import random
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

from .settings import settings


def get_conn():
    return pymysql.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )


def ensure_control_row(conn, proceso_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO vigilancia_control (proceso_id, next_run_at, fail_count)
            VALUES (%s, NOW(), 0)
            ON DUPLICATE KEY UPDATE proceso_id = proceso_id
            """,
            (proceso_id,),
        )


def fetch_due_processes(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        sql = """
        SELECT
          dip.id AS proceso_id,
          dip.radicado,
          dip.notify_first_actuation,
          dip.created_at,
          vc.next_run_at,
          vc.cooldown_until,
          vc.fail_count
        FROM despacho_ingreso_procesos dip
        JOIN vigilancia_control vc ON vc.proceso_id = dip.id
        WHERE dip.vigilancia_activa = 1
          AND (vc.cooldown_until IS NULL OR vc.cooldown_until <= NOW())
          AND (vc.next_run_at IS NULL OR vc.next_run_at <= NOW())
        ORDER BY COALESCE(vc.next_run_at, '1970-01-01') ASC
        LIMIT %s
        """
        cur.execute(sql, (settings.batch_size,))
        return cur.fetchall()


def count_actuaciones(conn, proceso_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM actuaciones_x_proceso
            WHERE proceso_id=%s AND fuente='CPNU'
            """,
            (proceso_id,),
        )
        return int(cur.fetchone()["c"])


def insert_actuacion_if_new(conn, proceso_id: int, hash_: str, row: Dict[str, Any]) -> bool:
    with conn.cursor() as cur:
        sql = """
        INSERT INTO actuaciones_x_proceso
        (proceso_id, fuente, hash, fecha_actuacion, actuacion, anotacion,
         fecha_inicia_termino, fecha_finaliza_termino, fecha_registro, raw_row_json)
        VALUES
        (%s, 'CPNU', %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cur.execute(
                sql,
                (
                    proceso_id,
                    hash_,
                    row.get("fecha_actuacion"),
                    row.get("actuacion"),
                    row.get("anotacion"),
                    row.get("fecha_inicia_termino"),
                    row.get("fecha_finaliza_termino"),
                    row.get("fecha_registro"),
                    json.dumps(row, ensure_ascii=False),
                ),
            )
            return True
        except pymysql.err.IntegrityError:
            return False


def insert_actuaciones_batch(
    conn,
    proceso_id: int,
    rows_with_hash: List[Tuple[str, Dict[str, Any]]],
) -> Tuple[int, List[str]]:
    inserted_hashes: List[str] = []
    inserted_count = 0

    for hash_, row in rows_with_hash:
        if insert_actuacion_if_new(conn, proceso_id, hash_, row):
            inserted_count += 1
            inserted_hashes.append(hash_)

    return inserted_count, inserted_hashes


def insert_worker_run_start(conn, proceso_id: int, fuente: str = "CPNU") -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO worker_runs (proceso_id, fuente, status) VALUES (%s, %s, %s)",
            (proceso_id, fuente, "RUNNING"),
        )
        return int(cur.lastrowid)


def update_worker_run_finish(
    conn,
    run_id: int,
    status: str,
    used_mode: Optional[str],
    rows_extracted: int,
    rows_inserted: int,
    notified: int,
    error_message: Optional[str],
    screenshot_path: Optional[str],
    html_path: Optional[str],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE worker_runs
            SET finished_at=NOW(),
                status=%s,
                used_mode=%s,
                rows_extracted=%s,
                rows_inserted=%s,
                notified=%s,
                error_message=%s,
                artifact_screenshot_path=%s,
                artifact_html_path=%s
            WHERE id=%s
            """,
            (
                status,
                used_mode,
                rows_extracted,
                rows_inserted,
                notified,
                error_message,
                screenshot_path,
                html_path,
                run_id,
            ),
        )


def _safe_len(value: Any) -> int:
    try:
        return len(value)
    except Exception:
        return 0


def compute_backoff_minutes(fail_count: int, error_code: str) -> int:
    code = (error_code or "").upper()

    if code in ("UI_SELECTOR", "UI_FLOW"):
        return 360 if fail_count < 3 else 720

    if code in ("SOFTBLOCK",):
        return 720 if fail_count < 2 else 1440

    if code in ("TIMEOUT", "NETWORK"):
        if fail_count <= 1:
            return 10
        if fail_count == 2:
            return 30
        if fail_count == 3:
            return 90
        return 360

    if fail_count <= 1:
        return 5
    if fail_count == 2:
        return 15
    if fail_count == 3:
        return 60
    return 360


def update_scheduler_success(conn, proceso_id: int) -> None:
    jitter = random.randint(0, 7)
    interval = int(getattr(settings, "interval_minutes", 60)) + jitter

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE vigilancia_control
            SET last_run_at=NOW(),
                last_success_at=NOW(),
                fail_count=0,
                cooldown_until=NULL,
                last_error_code=NULL,
                last_error_message=NULL,
                next_run_at=DATE_ADD(NOW(), INTERVAL %s MINUTE)
            WHERE proceso_id=%s
            """,
            (interval, proceso_id),
        )


def update_scheduler_failure(
    conn,
    proceso_id: int,
    error_code: str,
    error_message: str,
    current_fail_count: int,
) -> None:
    new_fail = int(current_fail_count or 0) + 1
    backoff_min = compute_backoff_minutes(new_fail, error_code)
    jitter = random.randint(0, 10)
    backoff_total = backoff_min + jitter

    msg = error_message or ""
    if _safe_len(msg) > 1500:
        msg = msg[:1500]

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE vigilancia_control
            SET last_run_at=NOW(),
                fail_count=%s,
                last_error_code=%s,
                last_error_message=%s,
                cooldown_until=DATE_ADD(NOW(), INTERVAL %s MINUTE),
                next_run_at=DATE_ADD(NOW(), INTERVAL %s MINUTE)
            WHERE proceso_id=%s
            """,
            (new_fail, error_code, msg, backoff_total, backoff_total, proceso_id),
        )


def parse_created_at(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None

def get_max_fecha_actuacion(conn, proceso_id: int) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(fecha_actuacion) AS m
            FROM actuaciones_x_proceso
            WHERE proceso_id=%s AND fuente='CPNU'
            """,
            (proceso_id,),
        )
        row = cur.fetchone()
        return row["m"] if row and row["m"] else None
