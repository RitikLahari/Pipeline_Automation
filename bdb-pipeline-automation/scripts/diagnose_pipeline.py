#!/usr/bin/env python
"""Read-only consistency diagnosis for any BDB standalone data pipeline."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
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
from bdb_pipeline.pipeline_service import PipelineService  # noqa: E402
from bdb_pipeline.utils import load_env_file  # noqa: E402


COMMON_FIELDS = (
    "componentInstanceId",
    "invocationMethodType",
    "batchWindow",
    "isComponentScheduled",
    "inEvent",
    "outEvent",
)


def _login(client: BDBClient) -> tuple[str, str, str]:
    email = str(os.environ.get("BDB_USER_EMAIL") or "").strip()
    password = str(os.environ.get("BDB_PASSWORD") or "").strip()
    user_id = str(os.environ.get("BDB_USER_ID") or "").strip()
    if not email or not password or not user_id:
        raise RuntimeError(
            "BDB_USER_EMAIL, BDB_PASSWORD, and BDB_USER_ID are required"
        )
    auth = AuthenticationService(client)
    customer_key = str(os.environ.get("BDB_CUSTOMERKEY") or "").strip()
    if not customer_key:
        customer_key = _customer_key_for_space(
            auth.get_customer_spaces(email), client.config.spacekey
        )
    token = extract_auth_token(
        auth.authenticate_user(email, password, customer_key)
    )
    return token, user_id, email


def _decode_workflow(workflow: dict[str, Any]) -> tuple[list[Any], dict[str, Any], dict[str, Any]]:
    try:
        forms = json.loads(
            base64.b64decode(workflow["com.pipeline"]).decode("utf-8")
        )
        current = json.loads(workflow["currentPipeline"])
        flowchart = json.loads(workflow["ui.flowchart"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Pipeline workflow could not be decoded") from exc
    if not isinstance(forms, list) or not isinstance(current, dict) or not isinstance(flowchart, dict):
        raise RuntimeError("Pipeline workflow has an unexpected shape")
    return forms, current, flowchart


def _event_names(component: dict[str, Any]) -> list[str]:
    result: list[str] = []
    incoming = component.get("inEvent")
    if isinstance(incoming, str) and incoming:
        result.append(incoming)
    outgoing = component.get("outEvent")
    if isinstance(outgoing, list):
        result.extend(str(item) for item in outgoing if item)
    return result


def _one(items: list[Any], predicate) -> Any | None:
    matches = [item for item in items if predicate(item)]
    return matches[0] if len(matches) == 1 else None


def diagnose_component(
    component: dict[str, Any],
    *,
    forms: list[Any],
    summaries: list[Any],
    operators: dict[str, Any],
    event_names: set[str],
    templates: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    name = str(component.get("displayName") or component.get("componentId") or "unknown")
    ui_id = component.get("UiId")
    issues: list[str] = []

    template = templates.get(str(component.get("componentId") or ""))
    supported_modes = {
        str(item.get("key"))
        for item in ((template or {}).get("componentUIMetaData") or {}).get(
            "invocationTypes", []
        )
        if isinstance(item, dict) and item.get("key")
    }
    mode = component.get("invocationMethodType")
    if supported_modes and mode not in supported_modes:
        issues.append(
            f"invocationMethodType {mode!r} is not advertised by the current template"
        )
    if mode == "batch" and not str(component.get("batchWindow") or "").strip():
        issues.append("batch invocation has an empty batchWindow")

    for event_name in _event_names(component):
        if event_name not in event_names:
            issues.append(f"references missing Kafka event {event_name!r}")

    form = _one(
        forms,
        lambda item: isinstance(item, dict) and item.get("id") == ui_id,
    )
    form_component = (
        form.get("data", {}).get("data") if isinstance(form, dict) else None
    )
    if not isinstance(form_component, dict):
        issues.append("missing or ambiguous com.pipeline form")
    else:
        for field in COMMON_FIELDS:
            if form_component.get(field) != component.get(field):
                issues.append(f"{field} differs between executable and form")

    summary = _one(
        summaries,
        lambda item: isinstance(item, dict)
        and item.get("componentInstanceId") == component.get("componentInstanceId"),
    )
    if not isinstance(summary, dict):
        issues.append("missing or ambiguous currentPipeline summary")
    else:
        for field in ("componentInstanceId", "invocationMethodType", "isComponentScheduled"):
            if field in summary and summary.get(field) != component.get(field):
                issues.append(f"{field} differs between executable and summary")

    operator = operators.get(ui_id) if isinstance(operators, dict) else None
    operator_type = operator.get("type") if isinstance(operator, dict) else None
    if not isinstance(operator_type, dict):
        issues.append("missing canvas operator")
    else:
        for field in ("componentInstanceId", "invocationMethodType", "isComponentScheduled"):
            if field in operator_type and operator_type.get(field) != component.get(field):
                issues.append(f"{field} differs between executable and canvas")

    return {
        "name": name,
        "componentId": component.get("componentId"),
        "componentInstanceId": component.get("componentInstanceId"),
        "invocationMethodType": mode,
        "batchWindow": component.get("batchWindow"),
        "events": _event_names(component),
        "issues": issues,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Diagnose component/event/workflow consistency for a BDB pipeline"
    )
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument(
        "--component",
        help="Optional exact display name, such as Sandbox Writer_1",
    )
    args = parser.parse_args()

    load_env_file()
    client = BDBClient(BDBConfig.from_env())
    token, user_id, email = _login(client)
    try:
        service = PipelineService(client)
        pipeline = service.get_pipeline_by_id(token, args.pipeline_id, user_id)["data"]
        workflow_response = service.get_pipeline_workflow(
            token, args.pipeline_id, user_id
        )
        if workflow_response["data"].get("pipelineId") != args.pipeline_id:
            raise RuntimeError("Workflow response belongs to a different pipeline")
        forms, current, flowchart = _decode_workflow(
            workflow_response["data"]["workFlowJson"]
        )
        events = service.list_events(token, args.pipeline_id, user_id)
        event_names = {
            str(item.get("eventName"))
            for item in events
            if isinstance(item, dict) and item.get("eventName")
        }
        catalog = service.get_all_components_info(token, user_id)
        templates = {
            str(item.get("componentId")): item
            for item in catalog
            if isinstance(item, dict) and item.get("componentId")
        }
        components = [
            item
            for item in pipeline.get("components") or []
            if isinstance(item, dict)
            and (not args.component or item.get("displayName") == args.component)
        ]
        if args.component and not components:
            raise RuntimeError(
                f"Pipeline does not contain component {args.component!r}"
            )
        results = [
            diagnose_component(
                component,
                forms=forms,
                summaries=current.get("components") or [],
                operators=flowchart.get("operators") or {},
                event_names=event_names,
                templates=templates,
            )
            for component in components
        ]
        print(
            json.dumps(
                {
                    "pipelineId": pipeline.get("pipelineId"),
                    "name": (pipeline.get("pipelineDefinition") or {}).get("name"),
                    "isRunning": pipeline.get("isRunning"),
                    "eventCount": len(event_names),
                    "componentCount": len(components),
                    "healthy": all(not item["issues"] for item in results),
                    "components": results,
                },
                indent=2,
            )
        )
        return 0 if all(not item["issues"] for item in results) else 3
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
