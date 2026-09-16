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
    parser = argparse.ArgumentParser(description="Read pipeline, pod, and UI-log status")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--records", type=int, default=25)
    parser.add_argument("--user-id", default="")
    args = parser.parse_args()
    try:
        client = BDBClient(BDBConfig.from_env())
        auth = auth_context_from_env(client, user_id_override=args.user_id)
        service = PipelineService(client)
        pipeline = service.get_pipeline_by_id(auth.token, args.pipeline_id, auth.user_id)
        pods = service.get_pod_details(auth.token, args.pipeline_id, auth.user_id)
        logs = service.get_pipeline_ui_logs(
            auth.token, args.pipeline_id, auth.user_id, args.records
        )
        pod_data = pods.get("data")
        if isinstance(pod_data, str):
            try:
                pod_data = json.loads(pod_data)
            except ValueError:
                pass
        print_json({"pipeline": pipeline, "pods": pod_data, "logs": logs})
        return 0
    except (BDBConfigurationError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
