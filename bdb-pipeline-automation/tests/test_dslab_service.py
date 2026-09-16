import base64
import json

import pytest

from bdb_pipeline.client import BDBConfig
from bdb_pipeline.dslab_service import (
    DSLabService,
    build_editor_state,
    build_register_payload,
    extract_notebook_id,
)


class FakeClient:
    def __init__(self, responses):
        self.config = BDBConfig("https://example.test", "5227")
        self.responses = list(responses)
        self.calls = []

    def post(self, path, data, headers):
        self.calls.append((path, data, headers))
        return self.responses.pop(0)


def _decoded_payload(call):
    return json.loads(base64.b64decode(call[1]["data"]).decode("utf-8"))


def test_extract_notebook_id_supports_nested_and_direct_shapes():
    assert extract_notebook_id({"notebook": '{"id": 123}'}) == "123"
    assert extract_notebook_id({"data": {"notebookId": "456"}}) == "456"
    assert extract_notebook_id({}) is None


def test_payload_builders_preserve_source_and_external_libraries():
    source = "def main(data):\n    return data"
    editor = build_editor_state(source)
    assert editor["code"][0]["code"] == source
    registered = build_register_payload(source, [{"name": "pandas"}])
    assert json.loads(registered["pipeline_content"]) == source
    assert json.loads(registered["externalLibraries"]) == [{"name": "pandas"}]


def test_register_python_script_sends_create_save_register_sequence():
    client = FakeClient(
        [
            {"success": True, "notebook": '{"id": "99"}'},
            {"success": True},
            {"success": True},
        ]
    )
    result = DSLabService(client).register_python_script(
        "secret-token",
        project_id="42",
        notebook_name="salary-script",
        source="def main(data):\n    return data",
        external_libraries=[],
        user_id="7",
    )

    assert result["notebookId"] == "99"
    assert len(client.calls) == 3
    assert [call[2]["docid"] for call in client.calls] == ["0", "42_99", "42_99"]
    assert all(call[0] == "/cxf/bizviz/pluginService" for call in client.calls)
    assert all(call[1]["consumerName"] == "NOTEBOOK" for call in client.calls)
    assert all(call[1]["serviceName"] == "saveNotebook" for call in client.calls)

    created, saved, registered = map(_decoded_payload, client.calls)
    assert created["projectId"] == "42"
    assert created["notebook"] == "salary-script"
    assert saved["id"] == "99"
    assert registered["id"] == "99"
    custom = json.loads(registered["customComponentscript"])
    assert json.loads(custom["pipeline_content"]).startswith("def main")


def test_register_python_script_rejects_missing_source():
    with pytest.raises(ValueError, match="source"):
        DSLabService(FakeClient([])).register_python_script(
            "token",
            project_id="42",
            notebook_name="name",
            source="",
        )
