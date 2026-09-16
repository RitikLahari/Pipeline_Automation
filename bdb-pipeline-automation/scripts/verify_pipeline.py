#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bdb_pipeline.pipeline_service import PipelineService, UnsupportedPipelineOperation  # noqa: E402
from bdb_pipeline.auth import auth_context_from_env  # noqa: E402
from bdb_pipeline.client import BDBClient, BDBConfig, BDBConfigurationError  # noqa: E402
from bdb_pipeline.utils import print_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify a BDB Data Pipeline")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--user-id", default="", help="BDB user ID sent in the request header")
    parser.add_argument(
        "--capabilities", action="store_true", help="Report discovered support without an API call"
    )
    args = parser.parse_args()
    if args.capabilities:
        print_json(PipelineService.capabilities())
        return 0
    try:
        client = BDBClient(BDBConfig.from_env())
        auth = auth_context_from_env(client, user_id_override=args.user_id)
        print_json(PipelineService(client).verify_pipeline(auth.token, args.pipeline_id, auth.user_id))
        return 0
    except (BDBConfigurationError, RuntimeError, ValueError, UnsupportedPipelineOperation) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
