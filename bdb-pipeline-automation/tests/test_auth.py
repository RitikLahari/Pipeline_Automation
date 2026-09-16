import pytest

from bdb_pipeline.auth import (
    AuthenticationService,
    auth_context_from_env,
    build_sso_login_url,
    extract_auth_token,
)
from bdb_pipeline.client import BDBConfig


class RecordingClient:
    def __init__(self):
        self.config = BDBConfig("https://app.bdb.ai", "1111")
        self.calls = []
        self.response = {"success": True}

    def post(self, path, data, headers=None):
        self.calls.append((path, data, headers))
        return self.response


def test_get_customer_spaces_uses_discovered_endpoint():
    client = RecordingClient()
    AuthenticationService(client).get_customer_spaces("user@example.com")
    assert client.calls == [
        (
            "/cxf/auth/getCustomerSpaces",
            {"userid": "user@example.com", "authType": "all"},
            None,
        )
    ]


def test_authenticate_uses_customer_key_and_space_header():
    client = RecordingClient()
    AuthenticationService(client).authenticate_user("u@example.com", "secret", "customer-1")
    path, body, headers = client.calls[0]
    assert path == "/cxf/auth/authenticateuser"
    assert body == {
        "userid": "u@example.com",
        "password": "secret",
        "authType": "ep",
        "customerkey": "customer-1",
    }
    assert headers == {"spacekey": "1111", "userid": "u@example.com"}


def test_authentication_rejects_blank_password_before_network():
    client = RecordingClient()
    with pytest.raises(ValueError, match="password"):
        AuthenticationService(client).authenticate_user("u@example.com", "", "customer-1")
    assert client.calls == []


def test_extract_auth_token_supports_known_shapes():
    assert extract_auth_token({"authToken": "top"}) == "top"
    assert extract_auth_token({"users": {"authToken": "nested"}}) == "nested"
    assert (
        extract_auth_token({"users": {"data": [{"AuthToken": "deeply-nested"}]}})
        == "deeply-nested"
    )


def test_extract_auth_token_rejects_ambiguous_values():
    with pytest.raises(RuntimeError, match="multiple"):
        extract_auth_token(
            {"authToken": "first", "users": {"authToken": "second"}}
        )


def test_sso_url_and_token_exchange_match_discovered_flow():
    url = build_sso_login_url(
        "https://app.bdb.ai/SSO/demo/login?x=1",
        "http://localhost:8000/callback",
    )
    assert url.startswith("https://app.bdb.ai/SSO/demo/login?")
    assert "redirect_url=http%3A%2F%2Flocalhost%3A8000%2Fcallback" in url

    client = RecordingClient()
    AuthenticationService(client).get_sso_user_token("short-lived")
    assert client.calls[0] == (
        "/cxf/auth/getSSOUserToken",
        {"ssoToken": "short-lived"},
        None,
    )


def test_auth_context_prefers_token_from_environment(monkeypatch):
    monkeypatch.setattr("bdb_pipeline.auth.load_env_file", lambda: None)
    monkeypatch.setenv("BDB_AUTH_TOKEN", "session-token")
    monkeypatch.setenv("BDB_USER_ID", "user-1")
    context = auth_context_from_env(RecordingClient(), prompt_for_token=False)
    assert context.token == "session-token"
    assert context.user_id == "user-1"


def test_auth_context_uses_stable_credentials_for_fresh_token(monkeypatch):
    monkeypatch.setattr("bdb_pipeline.auth.load_env_file", lambda: None)
    monkeypatch.delenv("BDB_AUTH_TOKEN", raising=False)
    monkeypatch.setenv("BDB_USER_EMAIL", "user@example.com")
    monkeypatch.setenv("BDB_PASSWORD", "local-secret")
    monkeypatch.setenv("BDB_CUSTOMERKEY", "customer-1")
    monkeypatch.setenv("BDB_USER_ID", "user-1")
    client = RecordingClient()
    client.response = {"authToken": "fresh-token", "id": "response-user"}

    context = auth_context_from_env(client, prompt_for_token=False)

    assert context.token == "fresh-token"
    assert context.user_id == "user-1"
    assert client.calls[0][0] == "/cxf/auth/authenticateuser"


def test_auth_context_discovers_customer_key_from_configured_space(monkeypatch):
    monkeypatch.setattr("bdb_pipeline.auth.load_env_file", lambda: None)
    monkeypatch.delenv("BDB_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("BDB_CUSTOMERKEY", raising=False)
    monkeypatch.setenv("BDB_USER_EMAIL", "user@example.com")
    monkeypatch.setenv("BDB_PASSWORD", "local-secret")
    monkeypatch.setenv("BDB_USER_ID", "user-1")

    class DiscoveryClient(RecordingClient):
        def post(self, path, data, headers=None):
            self.calls.append((path, data, headers))
            if path == "/cxf/auth/getCustomerSpaces":
                return {
                    "spaces": {
                        "nonSSOSpaces": [
                            {"spaceKey": 1111, "customerkey": "discovered-key"}
                        ],
                        "ssoConfigSpaces": [],
                    }
                }
            return {"authToken": "fresh-token", "id": "response-user"}

    client = DiscoveryClient()
    context = auth_context_from_env(client, prompt_for_token=False)

    assert context.token == "fresh-token"
    assert [call[0] for call in client.calls] == [
        "/cxf/auth/getCustomerSpaces",
        "/cxf/auth/authenticateuser",
    ]
    assert client.calls[1][1]["customerkey"] == "discovered-key"


def test_customer_key_discovery_rejects_wrong_space(monkeypatch):
    monkeypatch.setattr("bdb_pipeline.auth.load_env_file", lambda: None)
    monkeypatch.delenv("BDB_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("BDB_CUSTOMERKEY", raising=False)
    monkeypatch.setenv("BDB_USER_EMAIL", "user@example.com")
    monkeypatch.setenv("BDB_PASSWORD", "local-secret")
    client = RecordingClient()
    client.response = {
        "spaces": {
            "nonSSOSpaces": [{"spaceKey": 9999, "customerkey": "wrong-key"}]
        }
    }
    with pytest.raises(RuntimeError, match="BDB_SPACEKEY 1111"):
        auth_context_from_env(client, prompt_for_token=False)


def test_auth_context_supports_live_nested_spaces_shape(monkeypatch):
    monkeypatch.setattr("bdb_pipeline.auth.load_env_file", lambda: None)
    monkeypatch.delenv("BDB_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("BDB_CUSTOMERKEY", raising=False)
    monkeypatch.setenv("BDB_USER_EMAIL", "user@example.com")
    monkeypatch.setenv("BDB_PASSWORD", "local-secret")

    class LiveShapeClient(RecordingClient):
        def post(self, path, data, headers=None):
            self.calls.append((path, data, headers))
            if path == "/cxf/auth/getCustomerSpaces":
                return {
                    "spaces": {
                        "success": True,
                        "spaces": [
                            {"spaceKey": 6023, "customerKey": "other-key"},
                            {"spaceKey": 1111, "customerKey": "wanted-key"},
                        ],
                    }
                }
            return {"authToken": "fresh-token"}

    client = LiveShapeClient()
    context = auth_context_from_env(client, prompt_for_token=False)
    assert context.token == "fresh-token"
    assert client.calls[1][1]["customerkey"] == "wanted-key"


def test_auth_context_rejects_partial_password_configuration(monkeypatch):
    monkeypatch.setattr("bdb_pipeline.auth.load_env_file", lambda: None)
    for name in ("BDB_AUTH_TOKEN", "BDB_PASSWORD", "BDB_CUSTOMERKEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("BDB_USER_EMAIL", "user@example.com")
    with pytest.raises(RuntimeError, match="missing"):
        auth_context_from_env(RecordingClient(), prompt_for_token=False)
