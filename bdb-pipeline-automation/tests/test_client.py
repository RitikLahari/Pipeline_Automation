import os
from urllib.parse import parse_qs

import pytest

from bdb_pipeline.client import BDBClient, BDBConfig, BDBConfigurationError


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"success": True}


class FakeSession:
    def __init__(self):
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse()


def test_config_requires_environment(monkeypatch):
    monkeypatch.setattr("bdb_pipeline.client.load_env_file", lambda: None)
    monkeypatch.delenv("BDB_URL", raising=False)
    monkeypatch.delenv("BDB_SPACEKEY", raising=False)
    with pytest.raises(BDBConfigurationError, match="BDB_URL"):
        BDBConfig.from_env()


def test_config_loads_dotenv_without_overwriting_environment(monkeypatch):
    def fake_load_env_file():
        monkeypatch.setenv("BDB_SPACEKEY", "2222")
        monkeypatch.setenv(
            "BDB_URL", os.environ.get("BDB_URL", "https://from-file.example")
        )

    monkeypatch.setattr("bdb_pipeline.client.load_env_file", fake_load_env_file)
    monkeypatch.setenv("BDB_URL", "https://from-environment.example")
    monkeypatch.delenv("BDB_SPACEKEY", raising=False)
    config = BDBConfig.from_env()
    assert config.url == "https://from-environment.example"
    assert config.spacekey == "2222"


def test_config_rejects_non_http_url():
    with pytest.raises(BDBConfigurationError, match="absolute http"):
        BDBConfig("app.bdb.ai", "1111")


def test_client_uses_existing_form_encoded_pattern():
    session = FakeSession()
    client = BDBClient(BDBConfig("https://app.bdb.ai/", "1111"), session=session)

    result = client.post("/cxf/test", {"alpha": "one", "nested": '{"x":1}'})

    assert result == {"success": True}
    url, request = session.calls[0]
    assert url == "https://app.bdb.ai/cxf/test"
    assert parse_qs(request["data"]) == {"alpha": ["one"], "nested": ['{"x":1}']}
    assert request["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert request["timeout"] == 30.0


def test_client_rejects_relative_path():
    client = BDBClient(BDBConfig("https://app.bdb.ai", "1111"), session=FakeSession())
    with pytest.raises(ValueError, match="start with"):
        client.post("cxf/test", {})
