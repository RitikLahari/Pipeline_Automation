"""BDB authentication flow adapted from bdbservices/bdb-platform-sdk-python."""

from __future__ import annotations

import getpass
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .client import BDBClient
from .utils import load_env_file


@dataclass(frozen=True)
class AuthContext:
    token: str
    user_id: str


class AuthenticationService:
    """Password and token operations exposed by the existing BDB SDK.

    There is no API-key flow and no token-refresh endpoint in the inspected SDK.
    A caller obtains a new token by authenticating again when validation reports
    expiration. Credentials are accepted as arguments and are never logged.
    """

    def __init__(self, client: BDBClient) -> None:
        self.client = client

    def get_customer_spaces(self, userid: str, auth_type: str = "all") -> Any:
        _require_text(userid, "userid")
        return self.client.post(
            "/cxf/auth/getCustomerSpaces",
            {"userid": userid, "authType": auth_type},
        )

    def authenticate_user(
        self,
        userid: str,
        password: str,
        customerkey: str,
        auth_type: str = "ep",
    ) -> Any:
        _require_text(userid, "userid")
        _require_text(password, "password")
        _require_text(customerkey, "customerkey")
        return self.client.post(
            "/cxf/auth/authenticateuser",
            {
                "userid": userid,
                "password": password,
                "authType": auth_type,
                "customerkey": customerkey,
            },
            {"spacekey": self.client.config.spacekey, "userid": userid},
        )

    def get_user_info_by_token(self, token: str) -> Any:
        _require_text(token, "token")
        return self.client.post("/cxf/auth/getUserInfoByToken", {"token": token})

    def validate_token(self, token: str) -> Any:
        result = self.get_user_info_by_token(token)
        users = result.get("users") if isinstance(result, dict) else None
        if isinstance(users, dict) and users.get("success") is False:
            raise RuntimeError("Session expired")
        return result

    def get_sso_user_token(self, sso_token: str) -> Any:
        token = _require_text(sso_token, "sso_token")
        if any(char.isspace() for char in token):
            raise ValueError("sso_token must not contain whitespace")
        return self.client.post("/cxf/auth/getSSOUserToken", {"ssoToken": token})


def build_sso_login_url(ssologin_url: str, redirect_url: str) -> str:
    """Build the configured-space SSO URL exactly as the sibling SDK does."""
    scheme, netloc, path, query, fragment = _require_http_url(
        ssologin_url, "ssologin_url"
    )
    normalized_redirect = urlunsplit(_require_http_url(redirect_url, "redirect_url"))
    params = [
        (key, value)
        for key, value in parse_qsl(query, keep_blank_values=True)
        if key != "redirect_url"
    ]
    params.append(("redirect_url", normalized_redirect))
    return urlunsplit((scheme, netloc, path, urlencode(params), fragment))


def extract_auth_token(response: Any) -> str:
    """Extract one non-empty auth token from a platform response.

    Different BDB deployments wrap the authentication result at different
    depths. Match the exact ``authToken`` field name case-insensitively while
    walking only JSON objects/arrays; never infer a token from unrelated text.
    """
    if not isinstance(response, dict):
        raise RuntimeError("Authentication response was not an object")

    candidates: list[str] = []

    def collect(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                collect(item)
            return
        if not isinstance(value, dict):
            return
        for key, item in value.items():
            if str(key).casefold() == "authtoken" and isinstance(item, str):
                token = item.strip()
                if token:
                    candidates.append(token)
            elif isinstance(item, (dict, list)):
                collect(item)

    collect(response)
    unique = list(dict.fromkeys(candidates))
    if not unique:
        raise RuntimeError("Authentication response did not contain authToken")
    if len(unique) > 1:
        raise RuntimeError("Authentication response contained multiple authToken values")
    return unique[0]


def extract_user_id(response: Any) -> str:
    """Extract the user ID shape used by the sibling SDK's authentication flow."""
    if not isinstance(response, dict):
        return ""
    value = response.get("id") or response.get("userId")
    users = response.get("users")
    if not value and isinstance(users, dict):
        value = users.get("id") or users.get("userId")
    return str(value or "")


def auth_context_from_env(
    client: BDBClient,
    *,
    user_id_override: str = "",
    prompt_for_token: bool = True,
) -> AuthContext:
    """Resolve credentials from the Git-ignored local environment.

    Prefer a supplied ``BDB_AUTH_TOKEN``. Otherwise authenticate with email and
    password. ``BDB_CUSTOMERKEY`` may override tenant discovery; when omitted,
    resolve it from ``getCustomerSpaces`` using the configured space key.
    """
    load_env_file()
    user_id = str(user_id_override or os.environ.get("BDB_USER_ID") or "").strip()
    token = str(os.environ.get("BDB_AUTH_TOKEN") or "").strip()
    if token:
        return AuthContext(token=token, user_id=user_id)

    email = str(os.environ.get("BDB_USER_EMAIL") or "").strip()
    password = str(os.environ.get("BDB_PASSWORD") or "").strip()
    customerkey = str(os.environ.get("BDB_CUSTOMERKEY") or "").strip()
    if email or password or customerkey:
        missing = [
            name
            for name, value in (("BDB_USER_EMAIL", email), ("BDB_PASSWORD", password))
            if not value
        ]
        if missing:
            raise RuntimeError(
                "Incomplete password-flow configuration; missing: " + ", ".join(missing)
            )
        auth_service = AuthenticationService(client)
        if not customerkey:
            spaces_response = auth_service.get_customer_spaces(email)
            customerkey = _customer_key_for_space(spaces_response, client.config.spacekey)
        response = auth_service.authenticate_user(email, password, customerkey)
        return AuthContext(
            token=extract_auth_token(response),
            user_id=user_id or extract_user_id(response),
        )

    if prompt_for_token:
        token = getpass.getpass("BDB auth token (hidden): ").strip()
        if token:
            return AuthContext(token=token, user_id=user_id)
    raise RuntimeError(
        "No BDB credentials found. Set BDB_AUTH_TOKEN, or set BDB_USER_EMAIL "
        "and BDB_PASSWORD in .env."
    )


def _customer_key_for_space(response: Any, configured_spacekey: str) -> str:
    """Select the password-login customer key for one configured BDB space."""
    entries: list[dict[str, Any]] = []

    def collect(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                collect(item)
            return
        if not isinstance(value, dict):
            return
        has_space = "spaceKey" in value or "spacekey" in value
        has_customer = "customerkey" in value or "customerKey" in value
        if has_space and has_customer:
            entries.append(value)
        for nested in value.values():
            if isinstance(nested, (dict, list)):
                collect(nested)

    collect(response)

    wanted = str(configured_spacekey).strip()
    matches = [
        item
        for item in entries
        if str(item.get("spaceKey") or item.get("spacekey") or "").strip() == wanted
    ]
    if not matches:
        raise RuntimeError(
            f"getCustomerSpaces did not return configured BDB_SPACEKEY {wanted}"
        )
    password_matches = [
        item for item in matches if item.get("customerkey") or item.get("customerKey")
    ]
    if len(password_matches) != 1:
        raise RuntimeError(
            f"Could not uniquely resolve customer key for BDB_SPACEKEY {wanted}; "
            "set BDB_CUSTOMERKEY explicitly or use the configured SSO flow"
        )
    return str(
        password_matches[0].get("customerkey")
        or password_matches[0].get("customerKey")
    ).strip()


def _require_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value.strip()


def _require_http_url(value: Any, label: str):
    raw = _require_text(value, label)
    parts = urlsplit(raw)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError(f"{label} must be an absolute http(s) URL")
    return (parts.scheme, parts.netloc, parts.path or "/", parts.query, parts.fragment)
