"""Shared HTTP client adapted from bdbservices' Python SDK client pattern."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlencode, urlsplit

import requests

from .utils import load_env_file


class BDBConfigurationError(RuntimeError):
    """Raised when required BDB client configuration is invalid or missing."""


class BDBRequestError(RuntimeError):
    """Raised when the BDB service returns an HTTP or response-format error."""


@dataclass(frozen=True)
class BDBConfig:
    url: str
    spacekey: str
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        normalized = self.url.strip().rstrip("/")
        parts = urlsplit(normalized)
        if parts.scheme not in {"http", "https"} or not parts.netloc:
            raise BDBConfigurationError("BDB_URL must be an absolute http(s) URL")
        if not str(self.spacekey).strip():
            raise BDBConfigurationError("BDB_SPACEKEY must be a non-empty value")
        if self.timeout_seconds <= 0:
            raise BDBConfigurationError("timeout_seconds must be greater than zero")
        object.__setattr__(self, "url", normalized)
        object.__setattr__(self, "spacekey", str(self.spacekey).strip())

    @classmethod
    def from_env(cls) -> "BDBConfig":
        load_env_file()
        url = os.environ.get("BDB_URL")
        spacekey = os.environ.get("BDB_SPACEKEY")
        if not url:
            raise BDBConfigurationError("BDB_URL environment variable is not set")
        if not spacekey:
            raise BDBConfigurationError("BDB_SPACEKEY environment variable is not set")
        return cls(url=url, spacekey=spacekey)


class BDBClient:
    """Synchronous form-encoded BDB client.

    BDB's existing SDK uses POST requests, form URL encoding, an ``authtoken``
    header (not Bearer authentication), and a ``spacekey`` header. TLS
    verification remains requests' secure default; no custom SSL switch was
    found in the source implementation.
    """

    def __init__(self, config: BDBConfig, session: Any | None = None) -> None:
        self.config = config
        self._session = session or requests.Session()

    def post(
        self,
        path: str,
        data: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        if not path.startswith("/"):
            raise ValueError("BDB request path must start with '/'")
        request_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/plain, */*",
        }
        if headers:
            request_headers.update(headers)
        try:
            response = self._session.post(
                f"{self.config.url}{path}",
                data=urlencode(dict(data)),
                headers=request_headers,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise BDBRequestError(f"BDB request failed for {path}: {exc}") from exc
        try:
            return response.json()
        except ValueError as exc:
            raise BDBRequestError(f"BDB returned non-JSON content for {path}") from exc
