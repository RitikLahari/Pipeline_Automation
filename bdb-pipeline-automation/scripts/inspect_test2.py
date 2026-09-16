#!/usr/bin/env python
"""Read-only status/log inspection for the approved ``test 2`` pipeline."""

from __future__ import annotations

import json
import os
import sys
import base64
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdb_pipeline.client import BDBClient, BDBConfig  # noqa: E402
from bdb_pipeline.pipeline_service import PipelineService  # noqa: E402
from bdb_pipeline.utils import load_env_file  # noqa: E402

from repair_test2_dslab import PIPELINE_ID, PIPELINE_NAME, _login  # noqa: E402


def main() -> int:
    load_env_file()
    user_id = str(os.environ.get("BDB_USER_ID") or "").strip()
    client = BDBClient(BDBConfig.from_env())
    token, email = _login(client, user_id)
    try:
        service = PipelineService(client)
        pipeline = service.get_pipeline_by_id(token, PIPELINE_ID, user_id)["data"]
        writer_template = service.find_component_by_name(
            token, "Sandbox Writer", user_id
        )
        actual_name = (pipeline.get("pipelineDefinition") or {}).get("name")
        if actual_name != PIPELINE_NAME:
            raise RuntimeError("Pipeline ID no longer belongs to test 2")
        logs = service.get_pipeline_ui_logs(token, PIPELINE_ID, user_id, 500)
        events = service.list_events(token, PIPELINE_ID, user_id)
        workflow_response = service.get_pipeline_workflow(token, PIPELINE_ID, user_id)
        workflow = workflow_response["data"]["workFlowJson"]
        forms = json.loads(base64.b64decode(workflow["com.pipeline"]).decode("utf-8"))
        messages = [
            item.get("message")
            for item in logs.get("data") or []
            if isinstance(item, dict) and isinstance(item.get("message"), str)
        ]
        writer = next(
            item
            for item in pipeline.get("components") or []
            if item.get("displayName") == "Sandbox Writer_1"
        )
        writer_form = next(
            item.get("data", {}).get("data")
            for item in forms
            if item.get("id") == writer.get("UiId")
        )
        print(
            json.dumps(
                {
                    "pipelineId": pipeline.get("pipelineId"),
                    "name": actual_name,
                    "isRunning": pipeline.get("isRunning"),
                    "componentActions": [
                        {
                            "name": item.get("displayName"),
                            "action": item.get("action"),
                        }
                        for item in pipeline.get("components") or []
                    ],
                    "writer": {
                        "componentId": writer.get("componentId"),
                        "componentInstanceId": writer.get("componentInstanceId"),
                        "invocationMethodType": writer.get("invocationMethodType"),
                        "batchWindow": writer.get("batchWindow"),
                        "inEvent": writer.get("inEvent"),
                        "action": writer.get("action"),
                        "isDeploy": writer.get("isDeploy"),
                        "isComponentScheduled": writer.get("isComponentScheduled"),
                        "deploymentType": writer.get("deploymentType"),
                        "metadata": writer.get("componentMetaData"),
                    },
                    "writerWorkflowMatchesExecutable": writer_form == writer,
                    "writerTemplate": {
                        "deploymentType": writer_template.get("deploymentType"),
                        "invocationTypes": (
                            writer_template.get("componentUIMetaData") or {}
                        ).get("invocationTypes"),
                        "configurations": (
                            writer_template.get("componentUIMetaData") or {}
                        ).get("configurations"),
                    },
                    "writerWorkflow": {
                        "componentId": writer_form.get("componentId"),
                        "componentInstanceId": writer_form.get("componentInstanceId"),
                        "invocationMethodType": writer_form.get("invocationMethodType"),
                        "batchWindow": writer_form.get("batchWindow"),
                        "inEvent": writer_form.get("inEvent"),
                        "action": writer_form.get("action"),
                        "isDeploy": writer_form.get("isDeploy"),
                        "isComponentScheduled": writer_form.get("isComponentScheduled"),
                        "deploymentType": writer_form.get("deploymentType"),
                        "metadata": writer_form.get("componentMetaData"),
                    },
                    "events": [
                        {
                            "displayName": item.get("displayName"),
                            "eventName": item.get("eventName"),
                            "usedOutput": item.get("usedOutput"),
                            "maxOutput": item.get("maxOutput"),
                        }
                        for item in events
                    ],
                    "writerMessages": [
                        message
                        for message in messages
                        if "writer" in message.casefold()
                    ][:30],
                    "recentMessages": messages[:20],
                },
                indent=2,
            )
        )
        return 0
    finally:
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
