import base64
import json

import pytest

from bdb_pipeline.pipeline_service import (
    PipelineService,
    PipelineSpec,
)
from bdb_pipeline.utils import redact


def test_pipeline_spec_validation():
    with pytest.raises(ValueError, match="name"):
        PipelineSpec("")
    with pytest.raises(ValueError, match="resource allocation"):
        PipelineSpec("example", resource_allocation="giant")


def test_create_dry_run_uses_verified_request_sequence():
    result = PipelineService().create_pipeline(PipelineSpec("example"), dry_run=True)
    assert result["dryRun"] is True
    assert result["supported"] is True
    assert result["proposedInput"]["pipeline"]["name"] == "example"
    first = result["requestSequence"][0]
    assert first["serviceName"] == "createPipeline"
    assert first["data"]["pipelineId"] == "0"
    assert first["data"]["spaceKey"] == "<BDB_SPACEKEY>"
    assert first["data"]["resourceLimit"] == "low"
    assert first["data"]["isBatchPipeline"] is False
    assert first["data"]["scheduleAsBatch"] is None


def test_live_create_requires_client():
    with pytest.raises(RuntimeError, match="BDBClient"):
        PipelineService().create_pipeline(PipelineSpec("example"), token="token")


def test_live_create_chains_metadata_workflow_and_verification():
    from bdb_pipeline.client import BDBConfig

    class SequenceClient:
        def __init__(self):
            self.config = BDBConfig("https://app.bdb.ai", "5227")
            self.calls = []

        def post(self, path, data, headers=None):
            self.calls.append((path, data, headers))
            service_name = data["serviceName"]
            if service_name == "createPipeline":
                return {
                    "success": True,
                    "data": {
                        "pipelineId": "dp_123",
                        "pipelineDefinition": {"name": "testing", "desc": ""},
                        "components": [],
                        "spaceKey": "5227",
                        "resourceLimit": "low",
                        "isBatchPipeline": False,
                    },
                }
            if service_name == "createPipelineWorkflow":
                return {"success": True, "data": {"pipelineId": "dp_123"}}
            if service_name == "getPipelineById":
                return {
                    "success": True,
                    "data": {
                        "pipelineId": "dp_123",
                        "pipelineDefinition": {"name": "testing", "desc": ""},
                        "components": [],
                        "spaceKey": "5227",
                        "resourceLimit": "low",
                        "isBatchPipeline": False,
                        "isActive": True,
                        "isRunning": False,
                    },
                }
            raise AssertionError(service_name)

    client = SequenceClient()
    result = PipelineService(client).create_pipeline(
        PipelineSpec("testing"), token="hidden-token", user_id="user-1"
    )
    assert result["created"] is True
    assert result["pipelineId"] == "dp_123"
    assert [json.loads(call[1]["data"]) for call in client.calls] == [
        {
            "pipelineId": "0",
            "pipelineDefinition": {"name": "testing", "desc": ""},
            "enableLogs": False,
            "logDestinationType": "",
            "logType": "",
            "resourceLimit": "low",
            "components": [],
            "createdFrom": 1,
            "spaceKey": "5227",
            "isBatchPipeline": False,
            "scheduleAsBatch": None,
        },
        {
            "pipelineId": "dp_123",
            "workFlowJson": {
                "currentPipeline": json.dumps(
                    {
                        "pipelineId": "dp_123",
                        "pipelineDefinition": {"name": "testing", "desc": ""},
                        "components": [],
                        "spaceKey": "5227",
                        "resourceLimit": "low",
                        "isBatchPipeline": False,
                    },
                    separators=(",", ":"),
                ),
                "com.pipeline": "",
                "ui.event": "",
                "ui.flowchart": '{"operators":{},"links":{},"operatorTypes":{}}',
            },
            "spaceKey": "5227",
        },
        {"pipelineId": "dp_123"},
    ]


def test_configuration_requires_object_and_dry_run_is_safe():
    service = PipelineService()
    with pytest.raises(ValueError, match="non-empty"):
        service.configure_pipeline("pipeline-1", {}, dry_run=True)
    result = service.configure_pipeline(
        "pipeline-1", {"components": []}, dry_run=True
    )
    assert result["request"] is None
    assert result["proposedInput"]["pipelineId"] == "pipeline-1"

    complete = {
        "pipeline": {"pipelineId": "pipeline-1", "components": []},
        "workflow": {
            "pipelineId": "pipeline-1",
            "workFlowJson": {"currentPipeline": '{"pipelineId":"pipeline-1"}'},
        },
    }
    result = service.configure_pipeline("pipeline-1", complete, dry_run=True)
    assert result["supported"] is True
    assert [item["serviceName"] for item in result["requestSequence"]] == [
        "updatePipeline",
        "updatePipelineWorkflow",
        "getPipelineById",
        "getPipelineWorkflow",
    ]


def test_event_create_and_list_use_captured_contracts():
    event = {
        "pipelineId": "dp_123",
        "displayName": "event_1",
        "eventName": "event_1_generated",
    }
    client = RecordingPipelineClient({"success": True, "data": event})
    service = PipelineService(client)
    assert service.create_event(
        "hidden-token", "dp_123", "event_1", user_id="user-1"
    )["data"] == event
    _, body, _ = client.calls[0]
    assert body["serviceName"] == "createEvent"
    assert json.loads(body["data"]) == {
        "displayName": "event_1",
        "pipelineId": "dp_123",
        "partitions": 3,
        "maxOutput": 1,
        "isFailover": False,
        "eventDuration": "",
        "eventName": "",
        "isShared": False,
        "isMapped": False,
        "spaceKey": "5227",
    }

    client.response = {"success": True, "data": [event]}
    assert service.list_events("hidden-token", "dp_123", "user-1") == [event]
    assert client.calls[-1][1]["serviceName"] == "getAllEvents"


def test_update_pipeline_and_workflow_use_editor_base64_encoding():
    client = RecordingPipelineClient(
        {"success": True, "data": {"pipelineId": "dp_123"}}
    )
    service = PipelineService(client)
    pipeline = {"pipelineId": "dp_123", "components": [{"UiId": "ui-1"}]}
    service.update_pipeline("hidden-token", pipeline, "user-1")
    _, body, headers = client.calls[-1]
    assert body["serviceName"] == "updatePipeline"
    assert json.loads(base64.b64decode(body["data"])) == pipeline
    assert headers["docid"] == "123"

    workflow = {
        "pipelineId": "dp_123",
        "workFlowJson": {"currentPipeline": '{"pipelineId":"dp_123"}'},
    }
    service.update_pipeline_workflow("hidden-token", workflow, "user-1")
    _, body, headers = client.calls[-1]
    assert body["serviceName"] == "updatePipelineWorkflow"
    assert json.loads(base64.b64decode(body["data"])) == workflow
    assert headers["docid"] == "123"


def test_change_status_and_status_reads_use_captured_contracts():
    response = {
        "success": True,
        "data": {"pipelineId": "dp_123", "isRunning": True},
    }
    client = RecordingPipelineClient(response)
    service = PipelineService(client)
    service.change_pipeline_status("hidden-token", "dp_123", True, "user-1")
    _, body, headers = client.calls[-1]
    assert body["serviceName"] == "changeStatusPipeline"
    assert json.loads(body["data"]) == {"pipelineId": "dp_123", "isRunning": True}
    assert headers["docid"] == "123"

    client.response = {"success": True, "data": "[]"}
    service.get_pod_details("hidden-token", "dp_123", "user-1")
    assert client.calls[-1][1]["serviceName"] == "getPodDetails"
    assert json.loads(client.calls[-1][1]["data"]) == {
        "spaceKey": "5227",
        "pipelineId": "dp_123",
    }

    client.response = {"success": True, "data": []}
    service.get_pipeline_ui_logs("hidden-token", "dp_123", "user-1", 10)
    assert client.calls[-1][1]["serviceName"] == "getPipelineUILogs"
    assert json.loads(client.calls[-1][1]["data"]) == {
        "pipelineId": "dp_123",
        "records": 10,
    }


class RecordingPipelineClient:
    def __init__(self, response):
        from bdb_pipeline.client import BDBConfig

        self.config = BDBConfig("https://app.bdb.ai", "5227")
        self.response = response
        self.calls = []

    def post(self, path, data, headers=None):
        self.calls.append((path, data, headers))
        return self.response


def test_list_pipelines_uses_captured_privilege_contract():
    pipelines = [
        {
            "pipelineId": "dp_123",
            "pipelineDefinition": {"name": "Skill_1"},
            "resourceLimit": "low",
        }
    ]
    client = RecordingPipelineClient({"success": True, "data": pipelines})
    assert PipelineService(client).list_pipelines("hidden-token", "user-1") == pipelines
    path, body, headers = client.calls[0]
    assert path == "/cxf/datasource/loadPipelinePrivilege"
    assert body == {"id": 6, "spacekey": "5227"}
    assert headers == {
        "authtoken": "hidden-token",
        "spacekey": "5227",
        "userid": "user-1",
    }


def test_list_pipelines_rejects_invalid_response_entry():
    client = RecordingPipelineClient(
        {"success": True, "data": [{"pipelineId": "job_1"}]}
    )
    with pytest.raises(RuntimeError, match="invalid pipeline entry"):
        PipelineService(client).list_pipelines("token")


def test_get_pipeline_by_id_uses_captured_contract():
    response = {
        "success": True,
        "data": {
            "pipelineId": "dp_123",
            "pipelineDefinition": {"name": "existing-model-pipeline", "desc": ""},
            "components": [],
            "isActive": True,
            "isRunning": False,
            "isBatchPipeline": False,
            "resourceLimit": "low",
        },
    }
    client = RecordingPipelineClient(response)
    result = PipelineService(client).get_pipeline_by_id("hidden-token", "dp_123", "user-1")
    assert result == response
    path, body, headers = client.calls[0]
    assert path == "/cxf/bizviz/pluginService"
    assert body["consumerName"] == "BIZVIZPIPELINE"
    assert body["serviceName"] == "getPipelineById"
    assert json.loads(body["data"]) == {"pipelineId": "dp_123"}
    assert headers == {
        "authtoken": "hidden-token",
        "spacekey": "5227",
        "userid": "user-1",
    }


def test_verify_pipeline_summarizes_captured_response():
    client = RecordingPipelineClient({
        "success": True,
        "data": {
            "pipelineId": "dp_123",
            "pipelineDefinition": {"name": "model-flow"},
            "components": [{"id": "one"}],
            "isActive": True,
            "isRunning": False,
            "isBatchPipeline": False,
            "resourceLimit": "low",
        },
    })
    result = PipelineService(client).verify_pipeline("hidden-token", "dp_123", "user-1")
    assert result == {
        "verified": True,
        "pipelineId": "dp_123",
        "name": "model-flow",
        "isActive": True,
        "isRunning": False,
        "isBatchPipeline": False,
        "resourceLimit": "low",
        "componentCount": 1,
    }


def test_pipeline_read_rejects_unsuccessful_or_mismatched_response():
    client = RecordingPipelineClient({"success": False, "message": "not found"})
    with pytest.raises(RuntimeError, match="not found"):
        PipelineService(client).get_pipeline_by_id("token", "dp_123")

    client.response = {"success": True, "data": {"pipelineId": "dp_other"}}
    with pytest.raises(RuntimeError, match="different pipelineId"):
        PipelineService(client).get_pipeline_by_id("token", "dp_123")


def test_component_catalog_uses_captured_contract_and_finds_dslab_runner():
    dslab = {
        "componentId": "comp_dynamic",
        "name": "DSLab Runner",
        "componentGroup": "Machine Learning",
        "componentUIMetaData": {
            "invocationTypes": [
                {"label": "Real-Time", "key": "realtime"},
                {"label": "Batch", "key": "batch"},
            ],
            "properties": [{"key": "dsLabExecutionType"}],
        },
    }
    client = RecordingPipelineClient({"success": True, "data": [dslab]})
    service = PipelineService(client)

    assert service.get_dslab_runner_template("hidden-token", "user-1") == dslab
    path, body, headers = client.calls[0]
    assert path == "/cxf/bizviz/pluginService"
    assert body["consumerName"] == "BIZVIZPIPELINE"
    assert body["serviceName"] == "getAllComponentsInfo"
    assert json.loads(body["data"]) == {}
    assert headers["authtoken"] == "hidden-token"


def test_component_lookup_fails_clearly_when_name_is_missing():
    client = RecordingPipelineClient({"success": True, "data": []})
    with pytest.raises(LookupError, match="not found"):
        PipelineService(client).find_component_by_name("token", "DSLab Runner")


def test_get_component_config_for_pipeline_uses_captured_contract():
    component = {
        "invocationMethodType": "realtime",
        "componentInstId": "comp123_inst_1",
        "name": "DSLab Runner",
        "uiId": "ui-1",
        "componentName": "DSLab Runner_1",
        "configurationType": "low",
        "pipelineId": "dp_123",
    }
    client = RecordingPipelineClient({"success": True, "data": [component]})
    result = PipelineService(client).get_component_config_for_pipeline(
        "hidden-token", "dp_123", "user-1"
    )
    assert result == [component]
    path, body, headers = client.calls[0]
    assert path == "/cxf/bizviz/pluginService"
    assert body["serviceName"] == "getComponentConfigForPipeline"
    assert json.loads(body["data"]) == {"pipelineId": "dp_123"}
    assert headers["userid"] == "user-1"


def test_get_component_config_for_pipeline_rejects_mismatched_pipeline():
    client = RecordingPipelineClient(
        {"success": True, "data": [{"pipelineId": "dp_other"}]}
    )
    with pytest.raises(RuntimeError, match="different pipelineId"):
        PipelineService(client).get_component_config_for_pipeline(
            "token", "dp_123"
        )


def test_create_pipeline_workflow_preserves_nested_json_contract():
    response = {
        "success": True,
        "data": {"pipelineId": "dp_123", "workFlowJson": {}},
    }
    client = RecordingPipelineClient(response)
    current = {
        "pipelineId": "dp_123",
        "components": [],
        "pipelineDefinition": {"name": "Skill_1", "desc": "skill_1"},
        "spaceKey": "5227",
        "resourceLimit": "low",
    }

    assert (
        PipelineService(client).create_pipeline_workflow(
            "hidden-token", "dp_123", current, user_id="user-1"
        )
        == response
    )
    path, body, headers = client.calls[0]
    assert path == "/cxf/bizviz/pluginService"
    assert body["serviceName"] == "createPipelineWorkflow"
    outer_data = json.loads(body["data"])
    assert outer_data["pipelineId"] == "dp_123"
    assert outer_data["spaceKey"] == "5227"
    workflow = outer_data["workFlowJson"]
    assert json.loads(workflow["currentPipeline"]) == current
    assert json.loads(workflow["ui.flowchart"]) == {
        "operators": {},
        "links": {},
        "operatorTypes": {},
    }
    assert workflow["com.pipeline"] == ""
    assert workflow["ui.event"] == ""
    assert headers["userid"] == "user-1"


def test_create_pipeline_workflow_rejects_mismatched_id():
    client = RecordingPipelineClient({"success": True, "data": {}})
    with pytest.raises(ValueError, match="must match"):
        PipelineService(client).create_pipeline_workflow(
            "token", "dp_123", {"pipelineId": "dp_other"}
        )
    assert client.calls == []


def test_check_pipeline_available_new_versions_uses_captured_contract():
    response = {"success": True, "data": []}
    client = RecordingPipelineClient(response)
    assert (
        PipelineService(client).check_pipeline_available_new_versions(
            "hidden-token", "dp_123", "user-1"
        )
        == response
    )
    _, body, _ = client.calls[0]
    assert body["serviceName"] == "checkPipelineAvailableNewVersions"
    assert json.loads(body["data"]) == {"pipelineId": "dp_123"}


def test_get_pipeline_workflow_uses_captured_contract():
    response = {
        "success": True,
        "data": {
            "pipelineId": "dp_123",
            "workFlowJson": {
                "currentPipeline": json.dumps(
                    {"pipelineId": "dp_123", "components": []}
                ),
                "ui.flowchart": json.dumps(
                    {"operators": {}, "links": {}, "operatorTypes": {}}
                ),
                "ui.event": "[]",
                "com.pipeline": "",
            },
        },
    }
    client = RecordingPipelineClient(response)
    assert (
        PipelineService(client).get_pipeline_workflow(
            "hidden-token", "dp_123", "user-1"
        )
        == response
    )
    path, body, headers = client.calls[0]
    assert path == "/cxf/bizviz/pluginService"
    assert body["serviceName"] == "getPipelineWorkflow"
    assert json.loads(body["data"]) == {"pipelineId": "dp_123"}
    assert headers["userid"] == "user-1"


def test_get_pipeline_workflow_rejects_mismatched_nested_pipeline():
    client = RecordingPipelineClient(
        {
            "success": True,
            "data": {
                "pipelineId": "dp_123",
                "workFlowJson": {
                    "currentPipeline": '{"pipelineId":"dp_other"}',
                    "ui.flowchart": "{}",
                    "ui.event": "",
                },
            },
        }
    )
    with pytest.raises(RuntimeError, match="different pipelineId"):
        PipelineService(client).get_pipeline_workflow("token", "dp_123")


def test_pipeline_request_surfaces_uppercase_platform_error():
    client = RecordingPipelineClient({"Error": "Invalid space key"})
    with pytest.raises(RuntimeError, match="Invalid space key"):
        PipelineService(client).get_all_components_info("token")


def test_dry_run_redaction_covers_nested_component_credentials():
    value = {
        "component": {
            "password": "one",
            "accessKey": "two",
            "client-secret": "three",
            "spacekey": "not-a-secret",
        }
    }
    assert redact(value) == {
        "component": {
            "password": "***REDACTED***",
            "accessKey": "***REDACTED***",
            "client-secret": "***REDACTED***",
            "spacekey": "not-a-secret",
        }
    }
