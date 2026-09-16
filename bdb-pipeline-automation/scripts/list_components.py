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
    parser = argparse.ArgumentParser(description="List BDB pipeline component templates")
    parser.add_argument("--user-id", default="", help="BDB user ID sent in the request header")
    parser.add_argument("--name", help="Return one exact component name, such as DSLab Runner")
    args = parser.parse_args()
    try:
        client = BDBClient(BDBConfig.from_env())
        auth = auth_context_from_env(client, user_id_override=args.user_id)
        service = PipelineService(client)
        if args.name:
            print_json(service.find_component_by_name(auth.token, args.name, auth.user_id))
        else:
            components = service.get_all_components_info(auth.token, auth.user_id)
            print_json([
                {
                    "componentId": item.get("componentId"),
                    "name": item.get("name"),
                    "componentGroup": item.get("componentGroup"),
                    "invocationTypes": [
                        mode.get("key")
                        for mode in (item.get("componentUIMetaData") or {}).get("invocationTypes", [])
                        if isinstance(mode, dict)
                    ],
                }
                for item in components
            ])
        return 0
    except (BDBConfigurationError, LookupError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
