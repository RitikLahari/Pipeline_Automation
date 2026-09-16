#!/usr/bin/env python
"""Create and execute the approved Demo salary top-five BDB pipeline.

This command deliberately authenticates once and logs out in ``finally`` because
the Demo tenant enforces a very small active-session limit.
"""

from __future__ import annotations

import argparse
import base64
import copy
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdb_pipeline.auth import (  # noqa: E402
    AuthenticationService,
    _customer_key_for_space,
    extract_auth_token,
)
from bdb_pipeline.client import BDBClient, BDBConfig  # noqa: E402
from bdb_pipeline.dslab_service import DSLabService  # noqa: E402
from bdb_pipeline.pipeline_service import PipelineService, PipelineSpec  # noqa: E402
from bdb_pipeline.utils import load_env_file  # noqa: E402


SCRIPT_SOURCE = '''def main(data):
    import pandas as pd

    frame = data.copy() if hasattr(data, "copy") else pd.DataFrame(data)
    frame["Salary"] = pd.to_numeric(frame["Salary"], errors="coerce")
    return frame.sort_values("Salary", ascending=False, kind="stable").head(5)
'''


def _compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _ui_id() -> str:
    return "-".join(f"{random.randint(0, 0xFFFF):04x}" for _ in range(5))


def _instance_ids(offset: int) -> tuple[str, str]:
    stamp = int(time.time() * 1000) + offset
    return f"comp{stamp}_inst_{random.randint(1000, 9999)}", f"{stamp}_{random.randint(1000, 9999)}"


def _component(
    template: dict[str, Any],
    *,
    display_name: str,
    ui_id: str,
    instance_id: str,
    guid: str,
    invocation: str,
    metadata: dict[str, Any],
    spacekey: str,
    in_event: str = "",
    out_events: list[str] | None = None,
    batch_window: str = "",
) -> dict[str, Any]:
    result = {
        "containerImageVersion": template.get("version") or "",
        "componentDesc": "",
        "componentId": template["componentId"],
        "displayName": display_name,
        "UiId": ui_id,
        "containerImageName": template.get("imageName") or "",
        "onFailure": "",
        "isScheduled": False,
        "inEvent": in_event,
        "action": "stop",
        "gUid": guid,
        "componentInstanceId": instance_id,
        "invocationMethodType": invocation,
        "componentType": template.get("componentType") or "system",
        "componentMetaData": metadata,
        "alertAction": "",
        "isDeploy": True,
        "isDynamicAllocationEnabled": False,
        "batchWindow": batch_window,
        "isComponentScheduled": False,
        "cronExpression": "",
        "spaceKey": spacekey,
        "deploymentType": template.get("deploymentType") or "",
        "preConditionCheck": "None",
        "imagePullSecretRefs": [],
        "outEvent": out_events or [],
        "onSuccess": "",
    }
    if display_name.startswith("Sandbox Reader"):
        result["partitioningFactor"] = 1
    return result


def _component_operator(template: dict[str, Any], component: dict[str, Any], top: int, left: int) -> dict[str, Any]:
    operator_type = copy.deepcopy(template)
    operator_type.update(
        {
            "name": component["displayName"],
            "UiId": component["UiId"],
            "invocationMethodType": component["invocationMethodType"],
            "action": component["action"],
            "isComponentScheduled": False,
            "componentInstanceId": component["componentInstanceId"],
            "gUid": component["gUid"],
            "scheduleTime": "",
            "timeZone": "",
            "timeZoneString": "",
            "alertAction": component["alertAction"],
        }
    )
    return {
        "top": top,
        "left": left,
        "type": operator_type,
        "properties": {
            "title": component["displayName"],
            "inputs": {"input_0": {"label": "I_1"}},
            "outputs": {"output_0": {"label": "O_1"}},
        },
    }


def _event_operator(event: dict[str, Any], ui_id: str, top: int, left: int) -> tuple[dict[str, Any], dict[str, Any]]:
    event_ui = copy.deepcopy(event)
    event_ui.update(
        {
            "type": "Event",
            "componentUIMetaData": {
                "connector": {"endpoint": {"in": 1, "out": 1}},
                "operatorId": ui_id,
            },
            "UiId": ui_id,
            "name": event["displayName"],
        }
    )
    operator = {
        "top": top,
        "left": left,
        "type": event_ui,
        "properties": {
            "title": event["displayName"],
            "inputs": {"input_0": {"label": "I_1"}},
            "outputs": {"output_0": {"label": "O_1"}},
        },
    }
    return event_ui, operator


def _link(source: str, target: str) -> dict[str, Any]:
    return {
        "fromConnector": "output_0",
        "fromOperator": source,
        "fromSubConnector": 0,
        "toConnector": "input_0",
        "toOperator": target,
        "toSubConnector": 0,
    }


def _form_item(
    component: dict[str, Any],
    operator: dict[str, Any],
    input_components: list[dict[str, str]],
    output_components: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "id": component["UiId"],
        "data": {
            "uiJson": {
                "id": component["UiId"],
                "title": component["displayName"],
                "data": operator,
                "misc": {
                    "inputComponents": input_components,
                    "outputComponents": output_components,
                },
            },
            "data": component,
        },
    }


def _decode_pod_phases(response: dict[str, Any]) -> list[str]:
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
            phase = item.get("phase")
            if isinstance(phase, str):
                phases.append(phase)
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


def _contains_text(value: Any, text: str) -> bool:
    if isinstance(value, str):
        return text.casefold() in value.casefold()
    if isinstance(value, list):
        return any(_contains_text(item, text) for item in value)
    if isinstance(value, dict):
        return any(_contains_text(item, text) for item in value.values())
    return False


def _login_once(client: BDBClient, user_id: str) -> tuple[str, str]:
    service = AuthenticationService(client)
    email = str(os.environ.get("BDB_USER_EMAIL") or "").strip()
    password = str(os.environ.get("BDB_PASSWORD") or "").strip()
    if not email or not password:
        raise RuntimeError("BDB_USER_EMAIL and BDB_PASSWORD are required")
    spaces = service.get_customer_spaces(email)
    customer_key = str(os.environ.get("BDB_CUSTOMERKEY") or "").strip()
    if not customer_key:
        customer_key = _customer_key_for_space(spaces, client.config.spacekey)
    try:
        token = extract_auth_token(service.authenticate_user(email, password, customer_key))
    except RuntimeError:
        # Demo has a one-session limit. The user explicitly approved this reset.
        client.post(
            "/cxf/auth/resetUserSession",
            {"userId": user_id, "spaceKey": client.config.spacekey},
        )
        token = extract_auth_token(service.authenticate_user(email, password, customer_key))
    return token, email


def main() -> int:
    parser = argparse.ArgumentParser(description="Create and run a salary top-five BDB pipeline")
    parser.add_argument("--name", default="test 2")
    parser.add_argument(
        "--project-id",
        required=True,
        help="Explicit DS Lab project ID containing the published script",
    )
    parser.add_argument("--sandbox-id", type=int, default=64743328949)
    parser.add_argument("--source-file", default="Salary_Data.csv")
    parser.add_argument("--source-pipeline-id", default="dp_17891051242571106")
    parser.add_argument("--runtime-timeout", type=int, default=480)
    args = parser.parse_args()

    load_env_file()
    user_id = str(os.environ.get("BDB_USER_ID") or "").strip()
    if not user_id:
        raise RuntimeError("BDB_USER_ID is required")
    client = BDBClient(BDBConfig.from_env())
    token, email = _login_once(client, user_id)
    service = PipelineService(client)
    pipeline_id = ""
    activated = False
    script_id = ""
    writer_confirmed = False
    final_phases: list[str] = []

    try:
        print("[1/8] Verifying existing salary sandbox and DS Lab project...", flush=True)
        source = service.get_pipeline_by_id(token, args.source_pipeline_id, user_id)["data"]
        reader_match = next(
            (
                c
                for c in source.get("components") or []
                if (c.get("componentMetaData") or {}).get("sandboxReaderId") == args.sandbox_id
                and (c.get("componentMetaData") or {}).get("sandboxReaderTable") == args.source_file
            ),
            None,
        )
        runner_match = next(
            (
                c
                for c in source.get("components") or []
                if (c.get("componentMetaData") or {}).get("dsLabProjectId") == str(args.project_id)
            ),
            None,
        )
        if reader_match is None:
            raise RuntimeError("The requested Salary_Data.csv sandbox reference was not verified")
        if runner_match is None:
            raise RuntimeError("The requested DS Lab project reference was not verified")

        print("[2/8] Registering DS Lab script test_2_top5_salary...", flush=True)
        registration = DSLabService(client).register_python_script(
            token,
            project_id=str(args.project_id),
            notebook_name="test_2_top5_salary",
            source=SCRIPT_SOURCE,
            external_libraries=[],
            user_id=user_id,
        )
        script_id = str(registration.get("notebookId") or "")
        if not script_id:
            raise RuntimeError("BDB did not return a registered DS Lab script ID")

        print(f"[3/8] Creating low-resource pipeline {args.name!r}...", flush=True)
        created = service.create_pipeline(
            PipelineSpec(args.name, "Find the five highest salaries using DS Lab Script Runner", "low"),
            token=token,
            user_id=user_id,
            is_batch_pipeline=False,
        )
        pipeline_id = created["pipelineId"]
        pipeline = copy.deepcopy(created["pipeline"]["data"])

        print("[4/8] Creating and verifying two Kafka events...", flush=True)
        event1 = service.create_event(
            token,
            pipeline_id,
            "test_2_salary_input",
            user_id=user_id,
            partitions=3,
            max_output=1,
            event_duration="4",
        )["data"]
        event2 = service.create_event(
            token,
            pipeline_id,
            "test_2_salary_top5",
            user_id=user_id,
            partitions=3,
            max_output=1,
            event_duration="4",
        )["data"]
        listed_events = service.list_events(token, pipeline_id, user_id)
        listed_names = {item.get("eventName") for item in listed_events}
        if event1["eventName"] not in listed_names or event2["eventName"] not in listed_names:
            raise RuntimeError("Kafka event read-back verification failed")

        templates = {item.get("name"): item for item in service.get_all_components_info(token, user_id)}
        required = {"Sandbox Reader", "DSLab Runner", "Sandbox Writer"}
        if not required.issubset(templates):
            raise RuntimeError("One or more required BDB component templates are unavailable")

        reader_ui, event1_ui, runner_ui, event2_ui, writer_ui = (_ui_id() for _ in range(5))
        reader_inst, reader_guid = _instance_ids(1)
        runner_inst, runner_guid = _instance_ids(2)
        writer_inst, writer_guid = _instance_ids(3)

        reader = _component(
            templates["Sandbox Reader"],
            display_name="Sandbox Reader_1",
            ui_id=reader_ui,
            instance_id=reader_inst,
            guid=reader_guid,
            invocation="realtime",
            metadata={
                "sandboxReaderStorageType": "PlatForm",
                "sandboxReaderId": args.sandbox_id,
                "sandboxReaderTable": args.source_file,
                "sandboxReaderNetworkPath": "",
                "sandboxReaderSelectedCols": [],
                "sandboxReaderPartitionColumns": [],
                "sandboxReaderFileFormat": {
                    "fileTypeCSV": "CSV",
                    "header": True,
                    "inferSchema": True,
                    "multiline": "false",
                },
                "sandboxReaderQuery": "",
                "sandboxReaderLimit": "",
                "sandboxReaderSeparator": ",",
            },
            spacekey=client.config.spacekey,
            out_events=[event1["eventName"]],
        )
        runner = _component(
            templates["DSLab Runner"],
            display_name="DSLab Runner_1",
            ui_id=runner_ui,
            instance_id=runner_inst,
            guid=runner_guid,
            invocation="batch",
            metadata={
                "dsLabExecutionType": "dsLabScriptRunner",
                "dsLabFunInputType": "dataFrame",
                "dsLabProjectId": str(args.project_id),
                "dsLabScriptId": script_id,
                "dsLabExternalLibrary": "",
                "dsLabFunctionName": "main",
                "dsLabScript": SCRIPT_SOURCE,
                # The editor serializes iterable input arguments before saving.
                # Sending an array causes the DS Lab form loader's JSON.parse to
                # fail and can leave componentMetaData empty after a UI save.
                "dsLabInputData": "[]",
            },
            spacekey=client.config.spacekey,
            in_event=event1["eventName"],
            out_events=[event2["eventName"]],
            batch_window="10",
        )
        writer = _component(
            templates["Sandbox Writer"],
            display_name="Sandbox Writer_1",
            ui_id=writer_ui,
            instance_id=writer_inst,
            guid=writer_guid,
            invocation="realtime",
            metadata={
                "sandboxStorageType": "Platform",
                "sandboxWriterFileType": "csv",
                "sandboxWriterDisplayName": "test_2_top5_salary",
                "sandboxWriterTable": "test_2_top5_salary.csv",
                "sandboxWriterSaveMode": "overwrite",
                "sandboxWriterSchemaFileName": "",
                "sandboxWriterSchema": "",
                "sandboxWriterSelectedCols": [],
            },
            spacekey=client.config.spacekey,
            in_event=event2["eventName"],
        )

        pipeline["components"] = [reader, runner, writer]
        pipeline["isBatchPipeline"] = False
        pipeline["scheduleAsBatch"] = None

        reader_op = _component_operator(templates["Sandbox Reader"], reader, 250, 80)
        event1_obj, event1_op = _event_operator(event1, event1_ui, 250, 380)
        runner_op = _component_operator(templates["DSLab Runner"], runner, 250, 680)
        event2_obj, event2_op = _event_operator(event2, event2_ui, 250, 980)
        writer_op = _component_operator(templates["Sandbox Writer"], writer, 250, 1280)
        flowchart = {
            "operators": {
                reader_ui: reader_op,
                event1_ui: event1_op,
                runner_ui: runner_op,
                event2_ui: event2_op,
                writer_ui: writer_op,
            },
            "links": {
                "0": _link(reader_ui, event1_ui),
                "1": _link(event1_ui, runner_ui),
                "2": _link(runner_ui, event2_ui),
                "3": _link(event2_ui, writer_ui),
            },
            "operatorTypes": {},
        }
        forms = [
            _form_item(reader, reader_op, [], [{"key": event1_ui, "value": event1["displayName"]}]),
            _form_item(
                runner,
                runner_op,
                [{"key": event1_ui, "value": event1["displayName"]}],
                [{"key": event2_ui, "value": event2["displayName"]}],
            ),
            _form_item(writer, writer_op, [{"key": event2_ui, "value": event2["displayName"]}], []),
        ]
        workflow_current = {
            "logType": pipeline.get("logType", ""),
            "components": [
                {
                    "invocationMethodType": c["invocationMethodType"],
                    "UiId": c["UiId"],
                    "action": c["action"],
                    "gUid": c["gUid"],
                    "componentInstanceId": c["componentInstanceId"],
                    "isComponentScheduled": False,
                }
                for c in (reader, runner, writer)
            ],
            "logDestinationType": pipeline.get("logDestinationType", ""),
            "enableLogs": pipeline.get("enableLogs", False),
            "pipelineId": pipeline_id,
            "pipelineDefinition": pipeline["pipelineDefinition"],
            "resourceLimit": pipeline["resourceLimit"],
        }
        workflow = {
            "pipelineId": pipeline_id,
            "spaceKey": client.config.spacekey,
            "workFlowJson": {
                "currentPipeline": _compact(workflow_current),
                "com.pipeline": base64.b64encode(_compact(forms).encode()).decode(),
                "ui.event": _compact([event1_obj, event2_obj]),
                "ui.flowchart": _compact(flowchart),
            },
        }

        print("[5/8] Saving components and visual workflow, then reading both back...", flush=True)
        service.update_pipeline(token, pipeline, user_id)
        service.update_pipeline_workflow(token, workflow, user_id)
        saved = service.get_pipeline_by_id(token, pipeline_id, user_id)["data"]
        saved_workflow = service.get_pipeline_workflow(token, pipeline_id, user_id)
        if len(saved.get("components") or []) != 3:
            raise RuntimeError("Pipeline component read-back did not contain three components")
        saved_runner = next(
            (
                item
                for item in saved.get("components") or []
                if item.get("componentId") == templates["DSLab Runner"]["componentId"]
            ),
            None,
        )
        saved_runner_metadata = (saved_runner or {}).get("componentMetaData") or {}
        required_runner_values = {
            "dsLabExecutionType": "dsLabScriptRunner",
            "dsLabProjectId": str(args.project_id),
            "dsLabScriptId": script_id,
            "dsLabFunctionName": "main",
            "dsLabFunInputType": "dataFrame",
            "dsLabInputData": "[]",
        }
        if any(saved_runner_metadata.get(key) != value for key, value in required_runner_values.items()):
            raise RuntimeError("DS Lab Runner metadata was not retained by BDB")
        if saved_workflow["data"]["pipelineId"] != pipeline_id:
            raise RuntimeError("Pipeline workflow read-back verification failed")
        service.check_pipeline_available_new_versions(token, pipeline_id, user_id)

        print("[6/8] Activating pipeline and waiting for component pods...", flush=True)
        try:
            before = service.get_pod_details(token, pipeline_id, user_id)
            if "Terminating" in _decode_pod_phases(before):
                raise RuntimeError("A previous pipeline pod is still Terminating")
        except RuntimeError:
            pass
        service.activate_pipeline(pipeline_id, token=token, user_id=user_id)
        activated = True
        deadline = time.monotonic() + args.runtime_timeout
        last_report = ""
        while time.monotonic() < deadline:
            state = service.get_pipeline_by_id(token, pipeline_id, user_id)["data"]
            try:
                pod_response = service.get_pod_details(token, pipeline_id, user_id)
                final_phases = _decode_pod_phases(pod_response)
            except RuntimeError:
                final_phases = []
            report = f"running={state.get('isRunning')} phases={final_phases or ['not-reported']}"
            if report != last_report:
                print(f"      {report}", flush=True)
                last_report = report
            try:
                logs = service.get_pipeline_ui_logs(token, pipeline_id, user_id, 100)
                if _contains_text(logs, "Sandbox Writer successfully written data"):
                    writer_confirmed = True
                    break
                if _contains_text(logs, "successfully written data"):
                    writer_confirmed = True
                    break
            except RuntimeError:
                pass
            time.sleep(15)

        print("[7/8] Collecting final status and deactivating test resources...", flush=True)
        final_state = service.get_pipeline_by_id(token, pipeline_id, user_id)["data"]
        try:
            logs = service.get_pipeline_ui_logs(token, pipeline_id, user_id, 100)
            writer_confirmed = writer_confirmed or _contains_text(logs, "successfully written data")
        except RuntimeError:
            pass
        service.change_pipeline_status(token, pipeline_id, False, user_id)
        activated = False
        stopped = service.get_pipeline_by_id(token, pipeline_id, user_id)["data"]
        print("[8/8] Completed.", flush=True)
        print(
            json.dumps(
                {
                    "pipelineId": pipeline_id,
                    "name": args.name,
                    "resourceLimit": stopped.get("resourceLimit"),
                    "componentCount": len(stopped.get("components") or []),
                    "scriptId": script_id,
                    "outputSandboxFile": "test_2_top5_salary.csv",
                    "writerCompletionObserved": writer_confirmed,
                    "lastPodPhases": final_phases,
                    "isRunningAfterCleanup": stopped.get("isRunning"),
                    "isRunningBeforeCleanup": final_state.get("isRunning"),
                },
                indent=2,
            ),
            flush=True,
        )
        return 0 if writer_confirmed else 3
    finally:
        if activated and pipeline_id:
            try:
                service.change_pipeline_status(token, pipeline_id, False, user_id)
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
