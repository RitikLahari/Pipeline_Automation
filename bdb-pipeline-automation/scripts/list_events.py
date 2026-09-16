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
    parser = argparse.ArgumentParser(description="List Kafka events for a BDB pipeline")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--user-id", default="")
    args = parser.parse_args()
    try:
        client = BDBClient(BDBConfig.from_env())
        auth = auth_context_from_env(client, user_id_override=args.user_id)
        events = PipelineService(client).list_events(
            auth.token, args.pipeline_id, auth.user_id
        )
        print_json(events)
        return 0
    except (BDBConfigurationError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
