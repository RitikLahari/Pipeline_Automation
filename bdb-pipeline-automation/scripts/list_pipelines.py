#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bdb_pipeline.auth import auth_context_from_env  # noqa: E402
from bdb_pipeline.client import BDBClient, BDBConfig, BDBConfigurationError  # noqa: E402
from bdb_pipeline.pipeline_service import PipelineService  # noqa: E402
from bdb_pipeline.utils import print_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="List visible BDB Data Pipelines")
    parser.add_argument("--user-id", default="", help="BDB user ID sent in headers")
    parser.add_argument("--full", action="store_true", help="Print complete pipeline objects")
    args = parser.parse_args()
    try:
        client = BDBClient(BDBConfig.from_env())
        auth = auth_context_from_env(client, user_id_override=args.user_id)
        pipelines = PipelineService(client).list_pipelines(auth.token, auth.user_id)
        if args.full:
            print_json(pipelines)
        else:
            print_json([
                {
                    "pipelineId": item.get("pipelineId"),
                    "name": (item.get("pipelineDefinition") or {}).get("name"),
                    "resourceLimit": item.get("resourceLimit"),
                    "pipelineState": item.get("pipelineState"),
                    "isRunning": item.get("isRunning"),
                }
                for item in pipelines
            ])
        return 0
    except (BDBConfigurationError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
