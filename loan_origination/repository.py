from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List

from .config import DATA_DIR


class JsonRepository:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or DATA_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

    def _path(self, name: str) -> Path:
        return self.base_dir / name

    def _read_json(self, name: str, default: Any) -> Any:
        path = self._path(name)
        if not path.exists():
            return default
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_json(self, name: str, payload: Any) -> None:
        path = self._path(name)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    def _serialize(self, value: Any) -> Any:
        if is_dataclass(value):
            return asdict(value)
        if isinstance(value, dict):
            return {key: self._serialize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._serialize(item) for item in value]
        return value

    def upsert_loan_record(self, loan_id: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            loans = self._read_json("loans.json", {})
            current = loans.get(loan_id, {})
            current.update(self._serialize(payload))
            loans[loan_id] = current
            self._write_json("loans.json", loans)

    def append_item(self, name: str, payload: Any) -> None:
        with self._lock:
            items = self._read_json(name, [])
            items.append(self._serialize(payload))
            self._write_json(name, items)

    def read_items(self, name: str) -> List[Dict[str, Any]]:
        return self._read_json(name, [])

    def write_items(self, name: str, items: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._write_json(name, items)

