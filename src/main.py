import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .settings import settings
from .db import (
    get_conn,
    ensure_control_row,
    fetch_due_processes,
    count_actuaciones,
    insert_actuacion_if_new,
    insert_actuaciones_batch,
    insert_worker_run_start,
    update_worker_run_finish,
    update_scheduler_success,
    update_scheduler_failure,
    parse_created_at,
    get_max_fecha_actuacion,
)
from .cpnu_scraper import scrape_actuaciones_cpnu, CpnuScrapeError
from .normalize import make_hash

ART_SCREEN_DIR = os.path.join("artifacts", "screenshots")
ART_HTML_DIR = os.path.join("artifacts", "html")


def within_new_process_window(created_at: Optional[datetime]) -> bool:
    if not created_at:
        return False
    return (datetime.now() - created_at) <= timedelta(hours=settings.new_process_window_hours)


def decide_notified(is_momento0: bool, notify_first: bool, created_at: Optional[datetime], rows_inserted: int) -> int:
    if settings.dry_run or rows_inserted <= 0:
        return 0
    if is_momento0:
        return 1 if (notify_first and within_new_process_window(created_at)) else 0
    return 1


def run_one_process(conn, p: Dict[str, Any]) -> None:
    proceso_id = int(p["proceso_id"])
    radicado = str(p["radicado"])
    notify_first = int(p.get("notify_first_actuation") or 0) == 1
    created_at = parse_created_at(p.get("created_at"))
    fail_count = int(p.get("fail_count") or 0)

    run_id = insert_worker_run_start(conn, proceso_id, "CPNU")
    conn.commit()

    used_mode = None
    rows_extracted = 0
    rows_inserted = 0
    notified = 0
    screenshot_path = None
    html_path = None

    try:
        rows, used_mode = scrape_actuaciones_cpnu(radicado)
        rows_extracted = len(rows)

        existing = count_actuaciones(conn, proceso_id)
        is_momento0 = existing == 0

        if is_momento0:
            rows_to_process = rows[: int(settings.baseline_rows)]
            rows_with_hash: List[Tuple[str, Dict[str, Any]]] = []
            for r in rows_to_process:
                rr = dict(r)
                h = make_hash(radicado, rr)
                rows_with_hash.append((h, rr))

            inserted_count, _ = insert_actuaciones_batch(conn, proceso_id, rows_with_hash)
            rows_inserted = inserted_count

        else:
            max_rows = int(getattr(settings, "check_rows", 50))
            rows_to_process = rows[:max_rows]

            max_db = get_max_fecha_actuacion(conn, proceso_id)

            for r in rows_to_process:
                rr = dict(r)
                f = rr.get("fecha_actuacion")

                if max_db and f and f < max_db:
                    break

                h = make_hash(radicado, rr)
                if insert_actuacion_if_new(conn, proceso_id, h, rr):
                    rows_inserted += 1

        notified = decide_notified(is_momento0, notify_first, created_at, rows_inserted)

        update_worker_run_finish(
            conn,
            run_id,
            status="OK",
            used_mode=used_mode,
            rows_extracted=rows_extracted,
            rows_inserted=rows_inserted,
            notified=notified,
            error_message=None,
            screenshot_path=screenshot_path,
            html_path=html_path,
        )
        update_scheduler_success(conn, proceso_id)
        conn.commit()

    except CpnuScrapeError as e:
        conn.rollback()

        screenshot_path = getattr(e, "screenshot_path", None) or screenshot_path
        html_path = getattr(e, "html_path", None) or html_path

        update_worker_run_finish(
            conn,
            run_id,
            status=str(getattr(e, "code", "ERROR")),
            used_mode=used_mode,
            rows_extracted=rows_extracted,
            rows_inserted=rows_inserted,
            notified=0,
            error_message=str(getattr(e, "message", str(e))),
            screenshot_path=screenshot_path,
            html_path=html_path,
        )
        update_scheduler_failure(conn, proceso_id, str(getattr(e, "code", "ERROR")), str(getattr(e, "message", str(e))), fail_count)
        conn.commit()

    except Exception as e:
        conn.rollback()

        msg = str(e)
        update_worker_run_finish(
            conn,
            run_id,
            status="ERROR",
            used_mode=used_mode,
            rows_extracted=rows_extracted,
            rows_inserted=rows_inserted,
            notified=0,
            error_message=msg,
            screenshot_path=screenshot_path,
            html_path=html_path,
        )
        update_scheduler_failure(conn, proceso_id, "ERROR", msg, fail_count)
        conn.commit()


def bootstrap_control_rows(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM despacho_ingreso_procesos WHERE vigilancia_activa=1")
        ids = [int(r["id"]) for r in cur.fetchall()]
    for pid in ids:
        ensure_control_row(conn, pid)
    conn.commit()


def main() -> None:
    if not settings.db_name:
        raise RuntimeError("DB_NAME no está configurado en .env")

    conn = get_conn()
    try:
        bootstrap_control_rows(conn)

        due = fetch_due_processes(conn)
        if not due:
            print("No hay procesos pendientes (next_run_at/cooldown).")
            return

        print(f"Procesos a revisar: {len(due)} (DRY_RUN={settings.dry_run})")
        for p in due:
            print(f"- proceso_id={p['proceso_id']} radicado={p['radicado']}")
            run_one_process(conn, p)

        print("Ejecución terminada.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()