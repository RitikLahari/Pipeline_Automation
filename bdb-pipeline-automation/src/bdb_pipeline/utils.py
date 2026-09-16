"""Small dependency-free helpers shared by the local CLI tools."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping


SENSITIVE_KEYS = {
    "accesskey",
    "access_key",
    "api_key",
    "apikey",
    "authtoken",
    "authorization",
    "client_secret",
    "ingestion_secret",
    "password",
    "private_key",
    "secret",
    "sso_token",
    "token",
}


def load_env_file(path: str | Path = ".env") -> None:
    """Load simple KEY=VALUE entries without replacing existing environment values."""
    env_path = Path(path)
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def load_json_object(path: str | Path) -> dict[str, Any]:
    """Read a JSON file and require an object at its root."""
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def redact(value: Any) -> Any:
    """Return a recursively redacted copy suitable for logs and dry-run output."""
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if (
                normalized in SENSITIVE_KEYS
                or "password" in normalized
                or "secret" in normalized
                or normalized.endswith("token")
            ):
                result[key] = "***REDACTED***"
            else:
                result[key] = redact(item)
        return result
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def print_json(value: Any) -> None:
    print(json.dumps(redact(value), indent=2, sort_keys=True))
