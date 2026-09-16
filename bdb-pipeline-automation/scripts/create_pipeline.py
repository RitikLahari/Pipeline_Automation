#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bdb_pipeline.pipeline_service import (  # noqa: E402
    PipelineService,
    PipelineSpec,
    UnsupportedPipelineOperation,
)
from bdb_pipeline.auth import auth_context_from_env  # noqa: E402
from bdb_pipeline.client import BDBClient, BDBConfig, BDBConfigurationError  # noqa: E402
from bdb_pipeline.utils import print_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Plan creation of a BDB Data Pipeline")
    parser.add_argument("--name", required=True)
    parser.add_argument("--description", default="")
    parser.add_argument(
        "--resource-allocation", choices=("low", "medium", "high"), default="low"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch", action="store_true", help="Create a batch pipeline")
    parser.add_argument("--user-id", default="", help="BDB user ID sent in the request")
    args = parser.parse_args()
    try:
        spec = PipelineSpec(args.name, args.description, args.resource_allocation)
        if args.dry_run:
            result = PipelineService().create_pipeline(
                spec, is_batch_pipeline=args.batch, dry_run=True
            )
        else:
            client = BDBClient(BDBConfig.from_env())
            auth = auth_context_from_env(client, user_id_override=args.user_id)
            result = PipelineService(client).create_pipeline(
                spec,
                token=auth.token,
                user_id=auth.user_id,
                is_batch_pipeline=args.batch,
            )
        print_json(result)
        return 0
    except (BDBConfigurationError, RuntimeError, ValueError, UnsupportedPipelineOperation) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
