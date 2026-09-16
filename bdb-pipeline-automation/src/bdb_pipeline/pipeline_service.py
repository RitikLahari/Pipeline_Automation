"""Verified service boundary for standalone BDB Data Pipelines.

Browser captures and the deployed editor source verify pipeline creation,
reads, component discovery, version checks, and workflow persistence. Component
Kafka events, pipeline/workflow updates, activation, pod status, and UI logs
are also verified from the deployed editor and a successful Demo-tenant run.
"""

from __future__ import annotations

import base64
import json
from dataclasses import asdict, dataclass
from typing import Any

from .client import BDBClient


class UnsupportedPipelineOperation(RuntimeError):
    pass


@dataclass(frozen=True)
class PipelineSpec:
    name: str
    description: str = ""
    resource_allocation: str = "low"

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("pipeline name must be a non-empty string")
        if self.resource_allocation not in {"low", "medium", "high"}:
            raise ValueError("resource allocation must be low, medium, or high")

    def to_public_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["name"] = self.name.strip()
        return value


class PipelineService:
    """Validate and plan standalone pipeline requests without guessed API calls."""

    DISCOVERY_NOTE = (
        "Pipeline operation is not currently exposed by the discovered SDK/API. "
        "Official documentation describes the UI workflow but does not publish "
        "an endpoint or request payload contract."
    )

    def __init__(self, client: BDBClient | None = None) -> None:
        self.client = client

    @staticmethod
    def capabilities() -> dict[str, Any]:
        return {
            "resourceType": "standalone-data-pipeline",
            "create": "available",
            "configure": "available-with-complete-exported-payloads",
            "activate": "available",
            "deactivate": "available",
            "list": "available",
            "componentCatalog": "available",
            "componentInstances": "available",
            "createWorkflow": "available-for-existing-pipeline-id",
            "readWorkflow": "available",
            "availableVersionCheck": "available",
            "read": "available",
            "verify": "available",
            "events": "create-and-list-available",
            "run": "activation-deploys-realtime-pipelines",
            "status": "pipeline-pod-and-ui-log-reads-available",
            "jobWorkflowOperations": "available separately through JobService",
        }

    def list_pipelines(
        self, token: str, user_id: str = "", privilege_id: int = 6
    ) -> list[dict[str, Any]]:
        """List pipelines through the captured loadPipelinePrivilege contract."""
        if not isinstance(token, str) or not token.strip():
            raise ValueError("authentication token must be a non-empty string")
        if not isinstance(privilege_id, int) or isinstance(privilege_id, bool):
            raise ValueError("privilege_id must be an integer")
        if self.client is None:
            raise RuntimeError("a BDBClient is required for live pipeline requests")
        response = self.client.post(
            "/cxf/datasource/loadPipelinePrivilege",
            {"id": privilege_id, "spacekey": self.client.config.spacekey},
            {
                "authtoken": token,
                "spacekey": self.client.config.spacekey,
                "userid": str(user_id or ""),
            },
        )
        if not isinstance(response, dict):
            raise RuntimeError("loadPipelinePrivilege returned a non-object response")
        if response.get("success") is not True:
            message = (
                response.get("message")
                or response.get("error")
                or response.get("Error")
                or response.get("errorMessage")
                or response.get("errorCode")
                or response.get("messageCode")
                or "pipeline list request was not successful"
            )
            raise RuntimeError(f"loadPipelinePrivilege failed: {message}")
        data = response.get("data")
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise RuntimeError("loadPipelinePrivilege response did not contain a data array")
        if any(not str(item.get("pipelineId") or "").startswith("dp_") for item in data):
            raise RuntimeError("loadPipelinePrivilege returned an invalid pipeline entry")
        return data

    def get_all_components_info(
        self, token: str, user_id: str = ""
    ) -> list[dict[str, Any]]:
        """Fetch the pipeline component catalog through the captured contract."""
        response = self._pipeline_plugin_post(
            token,
            "getAllComponentsInfo",
            {},
            user_id=user_id,
        )
        data = response.get("data")
        if not isinstance(data, list):
            raise RuntimeError("getAllComponentsInfo response did not contain a data array")
        if not all(isinstance(component, dict) for component in data):
            raise RuntimeError("getAllComponentsInfo returned an invalid component entry")
        return data

    def find_component_by_name(
        self, token: str, name: str, user_id: str = ""
    ) -> dict[str, Any]:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("component name must be a non-empty string")
        wanted = name.strip().casefold()
        matches = [
            component
            for component in self.get_all_components_info(token, user_id)
            if str(component.get("name") or "").strip().casefold() == wanted
        ]
        if not matches:
            raise LookupError(f"BDB component not found: {name}")
        if len(matches) > 1:
            raise LookupError(f"BDB component name is ambiguous: {name}")
        return matches[0]

    def get_component_config_for_pipeline(
        self, token: str, pipeline_id: str, user_id: str = ""
    ) -> list[dict[str, Any]]:
        """Fetch instantiated component configuration for one pipeline."""
        pipeline_id = _require_id(pipeline_id)
        response = self._pipeline_plugin_post(
            token,
            "getComponentConfigForPipeline",
            {"pipelineId": pipeline_id},
            user_id=user_id,
        )
        data = response.get("data")
        if not isinstance(data, list):
            raise RuntimeError(
                "getComponentConfigForPipeline response did not contain a data array"
            )
        for component in data:
            if not isinstance(component, dict):
                raise RuntimeError(
                    "getComponentConfigForPipeline returned an invalid component entry"
                )
            if component.get("pipelineId") != pipeline_id:
                raise RuntimeError(
                    "getComponentConfigForPipeline returned a different pipelineId"
                )
        return data

    def get_dslab_runner_template(
        self, token: str, user_id: str = ""
    ) -> dict[str, Any]:
        component = self.find_component_by_name(token, "DSLab Runner", user_id)
        ui_metadata = component.get("componentUIMetaData")
        if not isinstance(ui_metadata, dict):
            raise RuntimeError("DSLab Runner did not contain componentUIMetaData")
        modes = {
            item.get("key")
            for item in ui_metadata.get("invocationTypes") or []
            if isinstance(item, dict)
        }
        if not {"realtime", "batch"}.issubset(modes):
            raise RuntimeError("DSLab Runner does not expose realtime and batch modes")
        return component

    def create_pipeline_workflow(
        self,
        token: str,
        pipeline_id: str,
        current_pipeline: dict[str, Any],
        *,
        user_id: str = "",
        flowchart: dict[str, Any] | None = None,
        pipeline_code: str = "",
        ui_event: str = "",
    ) -> dict[str, Any]:
        """Persist a captured workflow for an already-created pipeline.

        BDB expects ``currentPipeline`` and ``ui.flowchart`` to be JSON strings
        nested inside the outer form field's JSON string. This method performs
        that encoding but deliberately does not invent or create a pipeline ID.
        """
        pipeline_id = _require_id(pipeline_id)
        if not isinstance(current_pipeline, dict) or not current_pipeline:
            raise ValueError("current_pipeline must be a non-empty JSON object")
        if current_pipeline.get("pipelineId") != pipeline_id:
            raise ValueError("current_pipeline pipelineId must match pipeline_id")
        if flowchart is None:
            flowchart = {"operators": {}, "links": {}, "operatorTypes": {}}
        if not isinstance(flowchart, dict):
            raise ValueError("flowchart must be a JSON object")

        response = self._pipeline_plugin_post(
            token,
            "createPipelineWorkflow",
            {
                "pipelineId": pipeline_id,
                "workFlowJson": {
                    "currentPipeline": _compact_json(current_pipeline),
                    "com.pipeline": str(pipeline_code),
                    "ui.event": str(ui_event),
                    "ui.flowchart": _compact_json(flowchart),
                },
                "spaceKey": self.client.config.spacekey if self.client else "",
            },
            user_id=user_id,
        )
        data = response.get("data")
        if not isinstance(data, dict) or data.get("pipelineId") != pipeline_id:
            raise RuntimeError(
                "createPipelineWorkflow response did not confirm the pipelineId"
            )
        return response

    def check_pipeline_available_new_versions(
        self, token: str, pipeline_id: str, user_id: str = ""
    ) -> dict[str, Any]:
        """Run the captured post-save component-version availability check."""
        pipeline_id = _require_id(pipeline_id)
        return self._pipeline_plugin_post(
            token,
            "checkPipelineAvailableNewVersions",
            {"pipelineId": pipeline_id},
            user_id=user_id,
        )

    def list_events(
        self, token: str, pipeline_id: str, user_id: str = ""
    ) -> list[dict[str, Any]]:
        pipeline_id = _require_id(pipeline_id)
        response = self._pipeline_plugin_post(
            token, "getAllEvents", {"pipelineId": pipeline_id}, user_id=user_id
        )
        events = response.get("data")
        if not isinstance(events, list) or not all(isinstance(item, dict) for item in events):
            raise RuntimeError("getAllEvents response did not contain a data array")
        if any(item.get("pipelineId") != pipeline_id for item in events):
            raise RuntimeError("getAllEvents returned an event for a different pipelineId")
        return events

    def create_event(
        self,
        token: str,
        pipeline_id: str,
        display_name: str,
        *,
        user_id: str = "",
        partitions: int = 3,
        max_output: int = 1,
        event_duration: str = "",
        is_failover: bool = False,
        is_mapped: bool = False,
        is_shared: bool = False,
    ) -> dict[str, Any]:
        pipeline_id = _require_id(pipeline_id)
        if not isinstance(display_name, str) or not display_name.strip():
            raise ValueError("event display name must be a non-empty string")
        if not isinstance(partitions, int) or isinstance(partitions, bool) or not 1 <= partitions <= 100:
            raise ValueError("partitions must be an integer from 1 to 100")
        if not isinstance(max_output, int) or isinstance(max_output, bool) or not 1 <= max_output <= 10:
            raise ValueError("max_output must be an integer from 1 to 10")
        payload = {
            "displayName": display_name.strip(),
            "pipelineId": pipeline_id,
            "partitions": partitions,
            "maxOutput": max_output,
            "isFailover": bool(is_failover),
            "eventDuration": str(event_duration),
            "eventName": "",
            "isShared": bool(is_shared),
            "isMapped": bool(is_mapped),
            "spaceKey": self.client.config.spacekey if self.client else "",
        }
        response = self._pipeline_plugin_post(
            token, "createEvent", payload, user_id=user_id
        )
        data = response.get("data")
        if not isinstance(data, dict) or data.get("pipelineId") != pipeline_id:
            raise RuntimeError("createEvent response did not confirm the pipelineId")
        if not str(data.get("eventName") or ""):
            raise RuntimeError("createEvent response did not contain eventName")
        return response

    def update_pipeline(
        self, token: str, pipeline: dict[str, Any], user_id: str = ""
    ) -> dict[str, Any]:
        if not isinstance(pipeline, dict):
            raise ValueError("pipeline must be a JSON object")
        pipeline_id = _require_id(pipeline.get("pipelineId"))
        response = self._pipeline_plugin_post(
            token,
            "updatePipeline",
            pipeline,
            user_id=user_id,
            base64_data=True,
            docid=pipeline_id,
        )
        data = response.get("data")
        if not isinstance(data, dict) or data.get("pipelineId") != pipeline_id:
            raise RuntimeError("updatePipeline response did not confirm the pipelineId")
        return response

    def update_pipeline_workflow(
        self, token: str, workflow: dict[str, Any], user_id: str = ""
    ) -> dict[str, Any]:
        if not isinstance(workflow, dict):
            raise ValueError("workflow must be a JSON object")
        pipeline_id = _require_id(workflow.get("pipelineId"))
        workflow_json = workflow.get("workFlowJson")
        if not isinstance(workflow_json, dict):
            raise ValueError("workflow workFlowJson must be a JSON object")
        response = self._pipeline_plugin_post(
            token,
            "updatePipelineWorkflow",
            workflow,
            user_id=user_id,
            base64_data=True,
            docid=pipeline_id,
        )
        data = response.get("data")
        if not isinstance(data, dict) or data.get("pipelineId") != pipeline_id:
            raise RuntimeError("updatePipelineWorkflow did not confirm the pipelineId")
        return response

    def get_pipeline_workflow(
        self, token: str, pipeline_id: str, user_id: str = ""
    ) -> dict[str, Any]:
        """Read and validate the serialized visual workflow for a pipeline."""
        pipeline_id = _require_id(pipeline_id)
        response = self._pipeline_plugin_post(
            token,
            "getPipelineWorkflow",
            {"pipelineId": pipeline_id},
            user_id=user_id,
        )
        data = response.get("data")
        if not isinstance(data, dict) or data.get("pipelineId") != pipeline_id:
            raise RuntimeError("getPipelineWorkflow did not confirm the pipelineId")
        workflow = data.get("workFlowJson")
        if not isinstance(workflow, dict):
            raise RuntimeError("getPipelineWorkflow did not contain workFlowJson")
        current = _decode_json_field(workflow, "currentPipeline", dict)
        if current.get("pipelineId") != pipeline_id:
            raise RuntimeError("workflow currentPipeline returned a different pipelineId")
        _decode_json_field(workflow, "ui.flowchart", dict)
        event_value = workflow.get("ui.event")
        if event_value not in {None, ""}:
            _decode_json_field(workflow, "ui.event", list)
        return response

    def create_pipeline(
        self,
        spec: PipelineSpec,
        *,
        token: str = "",
        user_id: str = "",
        is_batch_pipeline: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Create pipeline metadata, its empty workflow, then verify it."""
        payload = self._new_pipeline_payload(spec, is_batch_pipeline)
        if dry_run:
            return {
                "dryRun": True,
                "operation": "create",
                "resourceType": "standalone-data-pipeline",
                "supported": True,
                "proposedInput": {"pipeline": spec.to_public_dict()},
                "requestSequence": [
                    {
                        "serviceName": "createPipeline",
                        "data": payload,
                    },
                    {
                        "serviceName": "createPipelineWorkflow",
                        "data": "derived from createPipeline response data and pipelineId",
                    },
                    {
                        "serviceName": "getPipelineById",
                        "data": "uses the returned pipelineId for verification",
                    },
                ],
            }

        response = self._pipeline_plugin_post(
            token, "createPipeline", payload, user_id=user_id
        )
        data = response.get("data")
        pipeline_id = data.get("pipelineId") if isinstance(data, dict) else None
        if not isinstance(pipeline_id, str) or not pipeline_id.startswith("dp_"):
            raise RuntimeError("createPipeline response did not contain a valid pipelineId")
        try:
            workflow = self.create_pipeline_workflow(
                token, pipeline_id, data, user_id=user_id
            )
        except Exception as exc:
            raise RuntimeError(
                f"Pipeline {pipeline_id} was created but its workflow creation failed: {exc}"
            ) from exc
        try:
            verification = self.verify_pipeline(token, pipeline_id, user_id)
        except Exception as exc:
            raise RuntimeError(
                f"Pipeline {pipeline_id} was created but verification failed: {exc}"
            ) from exc
        return {
            "created": True,
            "pipelineId": pipeline_id,
            "pipeline": response,
            "workflow": workflow,
            "verification": verification,
        }

    def _new_pipeline_payload(
        self, spec: PipelineSpec, is_batch_pipeline: bool
    ) -> dict[str, Any]:
        if not isinstance(is_batch_pipeline, bool):
            raise ValueError("is_batch_pipeline must be a boolean")
        spacekey = self.client.config.spacekey if self.client else "<BDB_SPACEKEY>"
        return {
            "pipelineId": "0",
            "pipelineDefinition": {
                "name": spec.name.strip(),
                "desc": spec.description,
            },
            "enableLogs": False,
            "logDestinationType": "",
            "logType": "",
            "resourceLimit": spec.resource_allocation,
            "components": [],
            "createdFrom": 1,
            "spaceKey": spacekey,
            "isBatchPipeline": is_batch_pipeline,
            "scheduleAsBatch": {} if is_batch_pipeline else None,
        }

    def configure_pipeline(
        self,
        pipeline_id: str,
        configuration: dict[str, Any],
        *,
        token: str = "",
        user_id: str = "",
        dry_run: bool = False,
    ) -> dict[str, Any]:
        pipeline_id = _require_id(pipeline_id)
        if not isinstance(configuration, dict) or not configuration:
            raise ValueError("configuration must be a non-empty JSON object")
        pipeline = configuration.get("pipeline")
        workflow = configuration.get("workflow")
        complete = isinstance(pipeline, dict) and isinstance(workflow, dict)
        plan = self._plan(
            "configure", {"pipelineId": pipeline_id, "configuration": configuration}
        )
        if dry_run:
            if complete:
                return {
                    **plan,
                    "supported": True,
                    "reason": "Verified deployed-editor contracts; no requests were sent.",
                    "request": None,
                    "requestSequence": [
                        {"serviceName": "updatePipeline", "encoding": "base64 JSON"},
                        {"serviceName": "updatePipelineWorkflow", "encoding": "base64 JSON"},
                        {"serviceName": "getPipelineById", "encoding": "plain JSON"},
                        {"serviceName": "getPipelineWorkflow", "encoding": "plain JSON"},
                    ],
                }
            return plan
        if not complete:
            raise ValueError(
                "live configuration requires pipeline and workflow JSON objects"
            )
        if pipeline.get("pipelineId") != pipeline_id or workflow.get("pipelineId") != pipeline_id:
            raise ValueError("pipeline and workflow pipelineId values must match pipeline_id")
        pipeline_response = self.update_pipeline(token, pipeline, user_id)
        workflow_response = self.update_pipeline_workflow(token, workflow, user_id)
        return {
            "configured": True,
            "pipelineId": pipeline_id,
            "pipeline": pipeline_response,
            "workflow": workflow_response,
            "verification": {
                "pipeline": self.get_pipeline_by_id(token, pipeline_id, user_id),
                "workflow": self.get_pipeline_workflow(token, pipeline_id, user_id),
            },
        }

    def activate_pipeline(
        self,
        pipeline_id: str,
        *,
        token: str = "",
        user_id: str = "",
        dry_run: bool = False,
    ) -> dict[str, Any]:
        _require_id(pipeline_id)
        plan = self._plan("activate", {"pipelineId": pipeline_id})
        if dry_run:
            return {
                **plan,
                "supported": True,
                "reason": "Verified deployed-editor contract; no request was sent.",
                "request": {
                    "serviceName": "changeStatusPipeline",
                    "data": {"pipelineId": pipeline_id, "isRunning": True},
                },
            }
        return self.change_pipeline_status(token, pipeline_id, True, user_id)

    def change_pipeline_status(
        self, token: str, pipeline_id: str, is_running: bool, user_id: str = ""
    ) -> dict[str, Any]:
        pipeline_id = _require_id(pipeline_id)
        if not isinstance(is_running, bool):
            raise ValueError("is_running must be a boolean")
        response = self._pipeline_plugin_post(
            token,
            "changeStatusPipeline",
            {"pipelineId": pipeline_id, "isRunning": is_running},
            user_id=user_id,
            docid=pipeline_id,
        )
        data = response.get("data")
        if not isinstance(data, dict) or data.get("pipelineId") != pipeline_id:
            raise RuntimeError("changeStatusPipeline did not confirm the pipelineId")
        if data.get("isRunning") is not is_running:
            raise RuntimeError("changeStatusPipeline did not confirm the requested state")
        return response

    def get_pod_details(
        self, token: str, pipeline_id: str, user_id: str = ""
    ) -> dict[str, Any]:
        pipeline_id = _require_id(pipeline_id)
        return self._pipeline_plugin_post(
            token,
            "getPodDetails",
            {"spaceKey": self.client.config.spacekey if self.client else "", "pipelineId": pipeline_id},
            user_id=user_id,
            docid=pipeline_id,
        )

    def get_pipeline_ui_logs(
        self, token: str, pipeline_id: str, user_id: str = "", records: int = 25
    ) -> dict[str, Any]:
        pipeline_id = _require_id(pipeline_id)
        if not isinstance(records, int) or isinstance(records, bool) or records <= 0:
            raise ValueError("records must be a positive integer")
        return self._pipeline_plugin_post(
            token,
            "getPipelineUILogs",
            {"pipelineId": pipeline_id, "records": records},
            user_id=user_id,
            docid=pipeline_id,
        )

    def get_pipeline_by_id(
        self, token: str, pipeline_id: str, user_id: str = ""
    ) -> dict[str, Any]:
        """Read a standalone pipeline through the captured BDB contract."""
        pipeline_id = _require_id(pipeline_id)
        if not isinstance(token, str) or not token.strip():
            raise ValueError("authentication token must be a non-empty string")
        if self.client is None:
            raise RuntimeError("a BDBClient is required for live pipeline reads")
        response = self._pipeline_plugin_post(
            token,
            "getPipelineById",
            {"pipelineId": pipeline_id},
            user_id=user_id,
        )
        data = response.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("getPipelineById response did not contain a data object")
        if data.get("pipelineId") != pipeline_id:
            raise RuntimeError("getPipelineById returned a different pipelineId")
        return response

    def _pipeline_plugin_post(
        self,
        token: str,
        service_name: str,
        data: dict[str, Any],
        *,
        user_id: str = "",
        base64_data: bool = False,
        docid: str = "",
    ) -> dict[str, Any]:
        if not isinstance(token, str) or not token.strip():
            raise ValueError("authentication token must be a non-empty string")
        if self.client is None:
            raise RuntimeError("a BDBClient is required for live pipeline requests")
        serialized = json.dumps(data, separators=(",", ":"))
        if base64_data:
            serialized = base64.b64encode(serialized.encode("utf-8")).decode("ascii")
        headers = {
            "authtoken": token,
            "spacekey": self.client.config.spacekey,
            "userid": str(user_id or ""),
        }
        if docid:
            headers["docid"] = str(docid).split("_", 1)[-1]
        response = self.client.post(
            "/cxf/bizviz/pluginService",
            {
                "consumerName": "BIZVIZPIPELINE",
                "serviceName": service_name,
                "spacekey": self.client.config.spacekey,
                "loggedUser": str(user_id or ""),
                "data": serialized,
            },
            headers,
        )
        if not isinstance(response, dict):
            raise RuntimeError(f"{service_name} returned a non-object response")
        if response.get("success") is not True:
            message = (
                response.get("message")
                or response.get("error")
                or response.get("Error")
                or "pipeline request was not successful"
            )
            raise RuntimeError(f"{service_name} failed: {message}")
        return response

    def verify_pipeline(
        self, token: str, pipeline_id: str, user_id: str = ""
    ) -> dict[str, Any]:
        response = self.get_pipeline_by_id(token, pipeline_id, user_id)
        data = response["data"]
        definition = data.get("pipelineDefinition")
        name = definition.get("name") if isinstance(definition, dict) else None
        return {
            "verified": True,
            "pipelineId": data["pipelineId"],
            "name": name,
            "isActive": data.get("isActive"),
            "isRunning": data.get("isRunning"),
            "isBatchPipeline": data.get("isBatchPipeline"),
            "resourceLimit": data.get("resourceLimit"),
            "componentCount": len(data.get("components") or []),
        }

    def _plan(self, operation: str, proposed_input: dict[str, Any]) -> dict[str, Any]:
        return {
            "dryRun": True,
            "operation": operation,
            "resourceType": "standalone-data-pipeline",
            "supported": False,
            "proposedInput": proposed_input,
            "request": None,
            "reason": self.DISCOVERY_NOTE,
        }

    def _raise_unsupported(self, operation: str) -> None:
        raise UnsupportedPipelineOperation(f"Cannot {operation} pipeline: {self.DISCOVERY_NOTE}")


def _require_id(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("pipeline id must be a non-empty string")
    return value.strip()


def _compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _decode_json_field(
    container: dict[str, Any], key: str, expected_type: type
) -> Any:
    raw = container.get(key)
    if not isinstance(raw, str):
        raise RuntimeError(f"workflow {key} was not a JSON string")
    try:
        value = json.loads(raw)
    except ValueError as exc:
        raise RuntimeError(f"workflow {key} contained invalid JSON") from exc
    if not isinstance(value, expected_type):
        raise RuntimeError(f"workflow {key} contained an unexpected JSON type")
    return value
