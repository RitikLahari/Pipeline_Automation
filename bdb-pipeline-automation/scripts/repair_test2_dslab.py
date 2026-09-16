#!/usr/bin/env python
"""Repair and verify only the DS Lab Runner in the existing ``test 2`` pipeline."""

from __future__ import annotations

import ast
import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdb_pipeline.auth import (  # noqa: E402
    AuthenticationService,
    _customer_key_for_space,
    extract_auth_token,
)
from bdb_pipeline.client import BDBClient, BDBConfig  # noqa: E402
from bdb_pipeline.pipeline_service import PipelineService  # noqa: E402
from bdb_pipeline.utils import load_env_file  # noqa: E402


PIPELINE_ID = "dp_17894583819527032"
PIPELINE_NAME = "test 2"
PROJECT_ID = "64442156895"
SCRIPT_ID = "65140505598"
SCRIPT_NAME = "test_2_top5_salary"
RUNNER_COMPONENT_ID = "comp_16449201586344539"


def _compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _plugin_post(
    client: BDBClient,
    token: str,
    user_id: str,
    consumer: str,
    service: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    response = client.post(
        "/cxf/bizviz/pluginService",
        {
            "consumerName": consumer,
            "serviceName": service,
            "spacekey": client.config.spacekey,
            "loggedUser": user_id,
            "data": _compact(data),
        },
        {
            "authtoken": token,
            "spacekey": client.config.spacekey,
            "userid": user_id,
        },
    )
    if not isinstance(response, dict):
        raise RuntimeError(f"{service} returned a non-object response")
    return response


def _truthy_success(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.casefold() == "true")


def _registered_source(
    client: BDBClient, token: str, user_id: str
) -> tuple[str, str]:
    """Read, but never modify, the already-published DS Lab script."""
    published = _plugin_post(
        client,
        token,
        user_id,
        "NOTEBOOK",
        "getAllPublishedNotebooks",
        {"projectId": PROJECT_ID},
    )
    if not _truthy_success(published.get("success")):
        raise RuntimeError("Could not read published DS Lab scripts")
    match = next(
        (
            item
            for item in published.get("notebooks") or []
            if str(item.get("id") or "") == SCRIPT_ID
            and str(item.get("notebookName") or "") == SCRIPT_NAME
        ),
        None,
    )
    if match is None:
        raise RuntimeError("The already-published test_2_top5_salary script was not found")

    response = _plugin_post(
        client,
        token,
        user_id,
        "NOTEBOOK",
        "getNotebookById",
        {"id": SCRIPT_ID},
    )
    if not _truthy_success(response.get("success")):
        raise RuntimeError("Could not read the published DS Lab script")
    notebook = response.get("notebook")
    if isinstance(notebook, str):
        notebook = json.loads(notebook)
    if not isinstance(notebook, dict):
        raise RuntimeError("Published DS Lab notebook response was invalid")
    custom = notebook.get("customComponentscript")
    if isinstance(custom, str):
        custom = json.loads(custom)
    if not isinstance(custom, dict):
        raise RuntimeError("Published notebook is not exported as a pipeline script")
    source: Any = custom.get("pipeline_content")
    if isinstance(source, str):
        try:
            decoded = json.loads(source)
            if isinstance(decoded, str):
                source = decoded
        except ValueError:
            pass
    if not isinstance(source, str) or not source.strip():
        raise RuntimeError("Published DS Lab script did not contain source code")
    tree = ast.parse(source)
    if not any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "main" for node in tree.body):
        raise RuntimeError("Published DS Lab script does not expose main")

    libraries: Any = custom.get("externalLibraries")
    if isinstance(libraries, str):
        try:
            libraries = json.loads(libraries)
        except ValueError:
            libraries = []
    library_names = []
    for item in libraries or []:
        if isinstance(item, dict) and item.get("name"):
            library_names.append(str(item["name"]))
        elif isinstance(item, str) and item:
            library_names.append(item)
    return source, ",".join(library_names)


def _decode_phases(response: dict[str, Any]) -> list[str]:
    value: Any = response.get("data")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return []
    phases: list[str] = []

    def walk(item: Any) -> None:
        if isinstance(item, list):
            for nested in item:
                walk(nested)
        elif isinstance(item, dict):
            if isinstance(item.get("phase"), str):
                phases.append(item["phase"])
            status = item.get("status")
            if isinstance(status, str):
                try:
                    walk(json.loads(status))
                except ValueError:
                    pass
            for key, nested in item.items():
                if key != "status" and isinstance(nested, (dict, list)):
                    walk(nested)

    walk(value)
    return list(dict.fromkeys(phases))


def _preview(
    client: BDBClient, token: str, user_id: str, event_name: str
) -> list[Any]:
    response = requests.get(
        f"{client.config.url}/api/v1/pipeline/getpreviewData/"
        f"{quote(event_name, safe='')}/latest",
        headers={
            "Accept": "application/json, text/plain, */*",
            "authtoken": token,
            "spacekey": client.config.spacekey,
            "userid": user_id,
            "docid": event_name,
        },
        timeout=client.config.timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("success") is not True:
        return []
    values = payload.get("data")
    if not isinstance(values, list):
        return []
    result: list[Any] = []
    for item in values:
        if isinstance(item, str):
            try:
                result.append(json.loads(item))
            except ValueError:
                result.append(item)
        else:
            result.append(item)
    return result


def _log_contains_since(value: Any, text: str, since_ms: int) -> bool:
    """Match a UI-log message emitted by the current activation only."""
    entries = value.get("data") if isinstance(value, dict) else None
    if not isinstance(entries, list):
        return False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            timestamp = int(entry.get("timestamp") or 0)
        except (TypeError, ValueError):
            continue
        message = entry.get("message")
        if (
            timestamp >= since_ms
            and isinstance(message, str)
            and text.casefold() in message.casefold()
        ):
            return True
    return False


def _login(client: BDBClient, user_id: str) -> tuple[str, str]:
    auth = AuthenticationService(client)
    email = str(os.environ.get("BDB_USER_EMAIL") or "").strip()
    password = str(os.environ.get("BDB_PASSWORD") or "").strip()
    spaces = auth.get_customer_spaces(email)
    customer_key = str(os.environ.get("BDB_CUSTOMERKEY") or "").strip()
    if not customer_key:
        customer_key = _customer_key_for_space(spaces, client.config.spacekey)
    token = extract_auth_token(auth.authenticate_user(email, password, customer_key))
    return token, email


def main() -> int:
    load_env_file()
    user_id = str(os.environ.get("BDB_USER_ID") or "").strip()
    if not user_id:
        raise RuntimeError("BDB_USER_ID is required")
    client = BDBClient(BDBConfig.from_env())
    token, email = _login(client, user_id)
    service = PipelineService(client)
    activated = False
    try:
        print("[1/6] Reading only the already-published DS Lab script...", flush=True)
        source, external_libraries = _registered_source(client, token, user_id)

        print("[2/6] Loading only pipeline test 2...", flush=True)
        pipeline = service.get_pipeline_by_id(token, PIPELINE_ID, user_id)["data"]
        if (pipeline.get("pipelineDefinition") or {}).get("name") != PIPELINE_NAME:
            raise RuntimeError("Pipeline ID no longer belongs to test 2")
        if pipeline.get("isRunning") is True:
            raise RuntimeError("test 2 must be inactive before its DS Lab component is repaired")
        workflow_response = service.get_pipeline_workflow(token, PIPELINE_ID, user_id)
        workflow_data = workflow_response["data"]["workFlowJson"]

        runners = [
            item
            for item in pipeline.get("components") or []
            if item.get("componentId") == RUNNER_COMPONENT_ID
        ]
        if len(runners) != 1:
            raise RuntimeError("test 2 must contain exactly one DS Lab Runner")
        runner = runners[0]
        metadata = {
            "dsLabExecutionType": "dsLabScriptRunner",
            "dsLabFunInputType": "dataFrame",
            "dsLabProjectId": PROJECT_ID,
            "dsLabScriptId": SCRIPT_ID,
            "dsLabExternalLibrary": external_libraries,
            "dsLabFunctionName": "main",
            "dsLabScript": source,
            "dsLabInputData": "[]",
        }
        runner["componentMetaData"] = metadata
        runner["invocationMethodType"] = "batch"
        runner["batchWindow"] = "10"
        runner["action"] = "stop"

        forms_raw = workflow_data.get("com.pipeline")
        if not isinstance(forms_raw, str) or not forms_raw:
            raise RuntimeError("test 2 workflow does not contain component forms")
        forms = json.loads(base64.b64decode(forms_raw).decode("utf-8"))
        form_matches = [item for item in forms if item.get("id") == runner.get("UiId")]
        if len(form_matches) != 1:
            raise RuntimeError("Could not uniquely locate DS Lab Runner in the workflow")
        form_component = form_matches[0].get("data", {}).get("data")
        if not isinstance(form_component, dict):
            raise RuntimeError("DS Lab Runner workflow form is invalid")
        form_component.update(
            {
                "componentMetaData": metadata,
                "invocationMethodType": "batch",
                "batchWindow": "10",
                "action": "stop",
            }
        )
        workflow = {
            "pipelineId": PIPELINE_ID,
            "spaceKey": client.config.spacekey,
            "workFlowJson": {
                "currentPipeline": workflow_data["currentPipeline"],
                "com.pipeline": base64.b64encode(_compact(forms).encode()).decode(),
                "ui.event": workflow_data["ui.event"],
                "ui.flowchart": workflow_data["ui.flowchart"],
            },
        }

        print("[3/6] Saving the corrected DS Lab metadata and reading it back...", flush=True)
        service.update_pipeline(token, pipeline, user_id)
        service.update_pipeline_workflow(token, workflow, user_id)
        saved = service.get_pipeline_by_id(token, PIPELINE_ID, user_id)["data"]
        saved_runner = next(
            item
            for item in saved.get("components") or []
            if item.get("componentId") == RUNNER_COMPONENT_ID
        )
        saved_metadata = saved_runner.get("componentMetaData") or {}
        required = {
            "dsLabExecutionType": "dsLabScriptRunner",
            "dsLabFunInputType": "dataFrame",
            "dsLabProjectId": PROJECT_ID,
            "dsLabScriptId": SCRIPT_ID,
            "dsLabFunctionName": "main",
            "dsLabInputData": "[]",
        }
        mismatches = {
            key: {"expected": value, "actual": saved_metadata.get(key)}
            for key, value in required.items()
            if saved_metadata.get(key) != value
        }
        if mismatches:
            raise RuntimeError(f"BDB did not retain corrected DS Lab metadata: {mismatches}")
        print("      DS Lab metadata retained by BDB.", flush=True)

        events = service.list_events(token, PIPELINE_ID, user_id)
        output_event = next(
            item for item in events if item.get("displayName") == "test_2_salary_top5"
        )
        output_event_name = str(output_event["eventName"])

        print("[4/6] Activating test 2 and waiting for DS Lab output...", flush=True)
        activation_started_ms = int(time.time() * 1000)
        service.activate_pipeline(PIPELINE_ID, token=token, user_id=user_id)
        activated = True
        deadline = time.monotonic() + 600
        last_phases: list[str] = []
        preview: list[Any] = []
        logs: Any = {}
        runner_confirmed = False
        writer_confirmed = False
        while time.monotonic() < deadline:
            try:
                last_phases = _decode_phases(
                    service.get_pod_details(token, PIPELINE_ID, user_id)
                )
            except RuntimeError:
                last_phases = []
            try:
                preview = _preview(client, token, user_id, output_event_name)
            except (requests.RequestException, ValueError):
                preview = []
            try:
                logs = service.get_pipeline_ui_logs(token, PIPELINE_ID, user_id, 200)
                runner_confirmed = runner_confirmed or _log_contains_since(
                    logs, "script run successfully", activation_started_ms
                )
                writer_confirmed = writer_confirmed or _log_contains_since(
                    logs, "successfully written data", activation_started_ms
                )
            except RuntimeError:
                pass
            print(
                "      "
                f"phases={last_phases or ['not-reported']} "
                f"previewRows={len(preview)} "
                f"runnerSuccess={runner_confirmed} writerSuccess={writer_confirmed}",
                flush=True,
            )
            # A downstream Sandbox Writer consumes the Kafka event. In that
            # topology an empty event preview is expected after consumption,
            # so the durable writer-success log is the completion signal.
            if len(preview) >= 5 or (runner_confirmed and writer_confirmed):
                break
            time.sleep(15)
        if len(preview) < 5 and not (runner_confirmed and writer_confirmed):
            raise RuntimeError(
                "DS Lab output was not durably verified; "
                f"last phases={last_phases}, logs={json.dumps(logs, default=str)[:2000]}"
            )

        print("[5/6] Deactivating test 2...", flush=True)
        service.change_pipeline_status(token, PIPELINE_ID, False, user_id)
        activated = False
        stopped = service.get_pipeline_by_id(token, PIPELINE_ID, user_id)["data"]

        print("[6/6] Verified result.", flush=True)
        print(
            json.dumps(
                {
                    "pipelineId": PIPELINE_ID,
                    "name": PIPELINE_NAME,
                    "dslabMetadataVerified": True,
                    "outputEvent": output_event_name,
                    "previewRowCount": len(preview),
                    "preview": preview[:5],
                    "runnerCompletionObserved": runner_confirmed,
                    "writerCompletionObserved": writer_confirmed,
                    "verificationMethod": (
                        "event-preview" if len(preview) >= 5 else "pipeline-ui-logs"
                    ),
                    "lastPodPhases": last_phases,
                    "isRunningAfterCleanup": stopped.get("isRunning"),
                },
                indent=2,
                default=str,
            ),
            flush=True,
        )
        return 0
    finally:
        if activated:
            try:
                service.change_pipeline_status(token, PIPELINE_ID, False, user_id)
            except Exception:
                pass
        try:
            client.post(
                "/cxf/auth/logout",
                {"userid": user_id or email, "token": token, "spacekey": client.config.spacekey},
                {
                    "authtoken": token,
                    "spacekey": client.config.spacekey,
                    "userID": user_id or email,
                },
            )
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
