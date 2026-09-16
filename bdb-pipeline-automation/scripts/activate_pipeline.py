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
    parser = argparse.ArgumentParser(description="Activate or deactivate a BDB Data Pipeline")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--deactivate", action="store_true")
    parser.add_argument("--user-id", default="", help="BDB user ID sent in the request")
    args = parser.parse_args()
    try:
        if args.dry_run:
            result = PipelineService().activate_pipeline(args.pipeline_id, dry_run=True)
            if args.deactivate:
                result["operation"] = "deactivate"
                result["proposedInput"]["isRunning"] = False
                result["request"]["data"]["isRunning"] = False
        else:
            client = BDBClient(BDBConfig.from_env())
            auth = auth_context_from_env(client, user_id_override=args.user_id)
            result = PipelineService(client).change_pipeline_status(
                auth.token, args.pipeline_id, not args.deactivate, auth.user_id
            )
        print_json(result)
        return 0
    except (BDBConfigurationError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
