from __future__ import annotations

import os
from pathlib import Path


_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "no", "n", "off"}


def _load_dotenv() -> None:
    dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        os.environ.setdefault(key, value)


_load_dotenv()


def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None:
        return str(default)
    value = value.strip()
    return value if value else str(default)


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return bool(default)

    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be one of: true/false, yes/no, 1/0, on/off")


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return int(default)
    return int(value.strip())


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return float(default)
    return float(value.strip())


def env_int_list(name: str, default: tuple[int, ...] | list[int]) -> list[int]:
    value = os.getenv(name)
    if value is None or not value.strip():
        return [int(item) for item in default]
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def env_str_tuple(name: str, default: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None or not value.strip():
        return tuple(str(item) for item in default)
    return tuple(item.strip() for item in value.split(",") if item.strip())
