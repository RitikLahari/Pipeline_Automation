#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bdb_pipeline.auth import auth_context_from_env  # noqa: E402
from bdb_pipeline.client import BDBClient, BDBConfig, BDBConfigurationError  # noqa: E402
from bdb_pipeline.pipeline_service import PipelineService  # noqa: E402
from bdb_pipeline.utils import print_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect a BDB pipeline visual workflow")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--user-id", default="", help="BDB user ID sent in the request")
    parser.add_argument("--full", action="store_true", help="Print the raw API response")
    args = parser.parse_args()
    try:
        client = BDBClient(BDBConfig.from_env())
        auth = auth_context_from_env(client, user_id_override=args.user_id)
        response = PipelineService(client).get_pipeline_workflow(
            auth.token, args.pipeline_id, auth.user_id
        )
        if args.full:
            print_json(response)
            return 0
        workflow = response["data"]["workFlowJson"]
        current = json.loads(workflow["currentPipeline"])
        flowchart = json.loads(workflow["ui.flowchart"])
        events = json.loads(workflow["ui.event"]) if workflow.get("ui.event") else []
        print_json(
            {
                "pipelineId": args.pipeline_id,
                "componentCount": len(current.get("components") or []),
                "operatorCount": len(flowchart.get("operators") or {}),
                "linkCount": len(flowchart.get("links") or {}),
                "eventCount": len(events),
            }
        )
        return 0
    except (BDBConfigurationError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
