#!/usr/bin/env python
"""Repair and verify only Sandbox Writer_1 in the existing ``test 2`` pipeline."""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdb_pipeline.client import BDBClient, BDBConfig  # noqa: E402
from bdb_pipeline.pipeline_service import PipelineService  # noqa: E402
from bdb_pipeline.utils import load_env_file  # noqa: E402

from repair_test2_dslab import (  # noqa: E402
    PIPELINE_ID,
    PIPELINE_NAME,
    _compact,
    _decode_phases,
    _log_contains_since,
    _login,
)


WRITER_COMPONENT_ID = "comp_16424008887458903"
WRITER_DISPLAY_NAME = "Sandbox Writer_1"
OUTPUT_EVENT_DISPLAY_NAME = "test_2_salary_top5"
EXPECTED_OUTPUT_FILE = "test_2_top5_salary.csv"


def _only(items: list[dict[str, Any]], predicate, label: str) -> dict[str, Any]:
    matches = [item for item in items if predicate(item)]
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one {label}; found {len(matches)}")
    return matches[0]


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
        print("[1/7] Reading only pipeline test 2...", flush=True)
        pipeline = service.get_pipeline_by_id(token, PIPELINE_ID, user_id)["data"]
        if (pipeline.get("pipelineDefinition") or {}).get("name") != PIPELINE_NAME:
            raise RuntimeError("Pipeline ID no longer belongs to test 2")
        if pipeline.get("isRunning") is True:
            raise RuntimeError("test 2 must be inactive before repairing its writer")

        events = service.list_events(token, PIPELINE_ID, user_id)
        output_event = _only(
            events,
            lambda item: item.get("displayName") == OUTPUT_EVENT_DISPLAY_NAME,
            "test 2 output event",
        )
        output_event_name = str(output_event.get("eventName") or "")
        if not output_event_name:
            raise RuntimeError("test 2 output event does not have a generated eventName")

        writer = _only(
            pipeline.get("components") or [],
            lambda item: item.get("componentId") == WRITER_COMPONENT_ID
            and item.get("displayName") == WRITER_DISPLAY_NAME,
            "Sandbox Writer_1 component",
        )
        metadata = writer.get("componentMetaData") or {}
        if metadata.get("sandboxWriterTable") != EXPECTED_OUTPUT_FILE:
            raise RuntimeError("Sandbox Writer_1 output file is not the approved test 2 file")
        if writer.get("inEvent") != output_event_name:
            raise RuntimeError("Sandbox Writer_1 is not connected to the test 2 output event")

        workflow_response = service.get_pipeline_workflow(token, PIPELINE_ID, user_id)
        workflow_data = workflow_response["data"]["workFlowJson"]
        forms = json.loads(base64.b64decode(workflow_data["com.pipeline"]).decode("utf-8"))
        writer_form = _only(
            forms,
            lambda item: item.get("id") == writer.get("UiId"),
            "Sandbox Writer_1 workflow form",
        )
        form_component = writer_form.get("data", {}).get("data")
        if not isinstance(form_component, dict):
            raise RuntimeError("Sandbox Writer_1 workflow form is invalid")

        current_pipeline = json.loads(workflow_data["currentPipeline"])
        current_writer = _only(
            current_pipeline.get("components") or [],
            lambda item: item.get("componentInstanceId")
            == writer.get("componentInstanceId"),
            "Sandbox Writer_1 workflow summary",
        )
        flowchart = json.loads(workflow_data["ui.flowchart"])
        writer_operator = (flowchart.get("operators") or {}).get(writer.get("UiId"))
        if not isinstance(writer_operator, dict) or not isinstance(
            writer_operator.get("type"), dict
        ):
            raise RuntimeError("Sandbox Writer_1 canvas operator is invalid")

        print("[2/7] Switching only Sandbox Writer_1 to realtime...", flush=True)
        for component in (writer, form_component):
            component.update(
                {
                    "invocationMethodType": "realtime",
                    "batchWindow": "",
                    "action": "stop",
                    "isComponentScheduled": False,
                    "isDeploy": True,
                    "inEvent": output_event_name,
                }
            )
        current_writer.update(
            {
                "invocationMethodType": "realtime",
                "action": "stop",
                "isComponentScheduled": False,
            }
        )
        writer_operator["type"].update(
            {
                "invocationMethodType": "realtime",
                "action": "stop",
                "isComponentScheduled": False,
            }
        )

        workflow = {
            "pipelineId": PIPELINE_ID,
            "spaceKey": client.config.spacekey,
            "workFlowJson": {
                "currentPipeline": _compact(current_pipeline),
                "com.pipeline": base64.b64encode(_compact(forms).encode()).decode(),
                "ui.event": workflow_data["ui.event"],
                "ui.flowchart": _compact(flowchart),
            },
        }

        print("[3/7] Saving and reading back only test 2...", flush=True)
        service.update_pipeline(token, pipeline, user_id)
        service.update_pipeline_workflow(token, workflow, user_id)
        saved = service.get_pipeline_by_id(token, PIPELINE_ID, user_id)["data"]
        saved_writer = _only(
            saved.get("components") or [],
            lambda item: item.get("componentId") == WRITER_COMPONENT_ID,
            "saved Sandbox Writer_1 component",
        )
        if (
            saved_writer.get("invocationMethodType") != "realtime"
            or saved_writer.get("inEvent") != output_event_name
            or (saved_writer.get("componentMetaData") or {}).get("sandboxWriterTable")
            != EXPECTED_OUTPUT_FILE
        ):
            raise RuntimeError("BDB did not retain the corrected Sandbox Writer settings")
        saved_workflow = service.get_pipeline_workflow(token, PIPELINE_ID, user_id)
        saved_forms = json.loads(
            base64.b64decode(
                saved_workflow["data"]["workFlowJson"]["com.pipeline"]
            ).decode("utf-8")
        )
        saved_form = _only(
            saved_forms,
            lambda item: item.get("id") == writer.get("UiId"),
            "saved Sandbox Writer_1 workflow form",
        )
        if (
            saved_form.get("data", {}).get("data", {}).get("invocationMethodType")
            != "realtime"
        ):
            raise RuntimeError("Workflow did not retain the corrected writer invocation mode")

        print("[4/7] Activating only test 2...", flush=True)
        activation_started_ms = int(time.time() * 1000)
        service.activate_pipeline(PIPELINE_ID, token=token, user_id=user_id)
        activated = True
        deadline = time.monotonic() + 600
        last_phases: list[str] = []
        runner_confirmed = False
        writer_started = False
        writer_confirmed = False
        logs: Any = {}
        while time.monotonic() < deadline:
            try:
                last_phases = _decode_phases(
                    service.get_pod_details(token, PIPELINE_ID, user_id)
                )
            except RuntimeError:
                last_phases = []
            try:
                logs = service.get_pipeline_ui_logs(token, PIPELINE_ID, user_id, 500)
                runner_confirmed = runner_confirmed or _log_contains_since(
                    logs, "script run successfully", activation_started_ms
                )
                writer_started = writer_started or _log_contains_since(
                    logs, "Sandbox Writer_1 Started", activation_started_ms
                )
                writer_confirmed = writer_confirmed or _log_contains_since(
                    logs, "successfully written data", activation_started_ms
                )
            except RuntimeError:
                pass
            print(
                "      "
                f"phases={last_phases or ['not-reported']} "
                f"runnerSuccess={runner_confirmed} "
                f"writerStarted={writer_started} writerSuccess={writer_confirmed}",
                flush=True,
            )
            if runner_confirmed and writer_confirmed:
                break
            time.sleep(15)
        if not (runner_confirmed and writer_confirmed):
            raise RuntimeError(
                "Sandbox Writer_1 did not complete in the bounded test run; "
                f"phases={last_phases}, logs={json.dumps(logs, default=str)[:3000]}"
            )

        print("[5/7] Deactivating test 2...", flush=True)
        service.change_pipeline_status(token, PIPELINE_ID, False, user_id)
        activated = False
        stopped = service.get_pipeline_by_id(token, PIPELINE_ID, user_id)["data"]

        print("[6/7] Writer repair verified.", flush=True)
        print(
            json.dumps(
                {
                    "pipelineId": PIPELINE_ID,
                    "name": PIPELINE_NAME,
                    "writerInvocationMethodType": saved_writer.get(
                        "invocationMethodType"
                    ),
                    "writerInputEvent": saved_writer.get("inEvent"),
                    "outputFile": EXPECTED_OUTPUT_FILE,
                    "runnerCompletionObserved": runner_confirmed,
                    "writerStartedObserved": writer_started,
                    "writerCompletionObserved": writer_confirmed,
                    "lastPodPhases": last_phases,
                    "isRunningAfterCleanup": stopped.get("isRunning"),
                },
                indent=2,
            ),
            flush=True,
        )
        print("[7/7] API session logout follows.", flush=True)
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
                {
                    "userid": user_id or email,
                    "token": token,
                    "spacekey": client.config.spacekey,
                },
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
