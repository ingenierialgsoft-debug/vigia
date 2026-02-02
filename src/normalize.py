import json
import re
import hashlib
from typing import Any, Dict

def norm_text(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_hash(radicado: str, row: Dict[str, Any]) -> str:
    parts = [
        norm_text(radicado),
        norm_text(row.get("fecha_actuacion")),
        norm_text(row.get("actuacion")),
        norm_text(row.get("anotacion")),
        norm_text(row.get("fecha_inicia_termino")),
        norm_text(row.get("fecha_finaliza_termino")),
        norm_text(row.get("fecha_registro")),
    ]
    base = "|".join(parts)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def row_to_json(row: Dict[str, Any]) -> str:
    # guarda el row normalizado en JSON para auditoría
    safe = {k: norm_text(v) for k, v in row.items() if k != "raw_row_json"}
    return json.dumps(safe, ensure_ascii=False)
