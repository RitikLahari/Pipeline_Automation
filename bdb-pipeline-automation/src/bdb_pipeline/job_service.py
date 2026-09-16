"""Verified BDB Data Engineering job/workflow operations.

These calls are a minimal adaptation of bdbservices' ``job_service.py``. They
operate on BDB Jobs, not on the standalone Data Pipelines resource shown under
Data Engineering > Pipelines.
"""

from __future__ import annotations

import base64
import json
import re
from typing import Any

from .client import BDBClient


def job_docid(job_id: Any) -> str:
    return re.sub(r"^job_", "", str(job_id or ""))


class JobService:
    """Thin wrapper over the job operations verified in the existing SDK."""

    CONSUMER = "BIZVIZPIPELINE"

    def __init__(self, client: BDBClient) -> None:
        self.client = client

    def _plugin_post(
        self,
        token: str,
        service_name: str,
        data: dict[str, Any],
        *,
        user_id: str = "",
        docid: str = "0",
        base64_data: bool = False,
    ) -> Any:
        serialized = json.dumps(data)
        if base64_data:
            serialized = base64.b64encode(serialized.encode("utf-8")).decode("ascii")
        return self.client.post(
            "/cxf/bizviz/pluginService",
            {
                "serviceName": service_name,
                "consumerName": self.CONSUMER,
                "spacekey": self.client.config.spacekey,
                "loggedUser": user_id,
                "data": serialized,
            },
            {
                "authtoken": token,
                "spacekey": self.client.config.spacekey,
                "userID": user_id,
                "docid": docid,
            },
        )

    def get_job_by_id(self, token: str, job_id: str, user_id: str = "") -> Any:
        return self._plugin_post(
            token,
            "getJobById",
            {"jobId": job_id},
            user_id=user_id,
            docid=job_docid(job_id),
        )

    def get_job_workflow(self, token: str, job_id: str, user_id: str = "") -> Any:
        return self._plugin_post(
            token,
            "getJobWorkflow",
            {"jobId": job_id},
            user_id=user_id,
            docid=job_docid(job_id),
        )

    def change_status_job(
        self, token: str, job_id: str, is_running: bool, user_id: str = ""
    ) -> Any:
        return self._plugin_post(
            token,
            "changeStatusJob",
            {"isRunning": bool(is_running), "jobId": job_id},
            user_id=user_id,
            docid=job_docid(job_id),
        )

    def get_job_ui_logs(
        self, token: str, job_id: str, user_id: str = "", records: int = 25
    ) -> Any:
        if records <= 0:
            raise ValueError("records must be greater than zero")
        return self._plugin_post(
            token,
            "getJobUILogs",
            {"jobId": job_id, "records": records},
            user_id=user_id,
            docid=job_docid(job_id),
        )

    def get_advance_logs(self, token: str, job_id: str, user_id: str = "") -> Any:
        return self._plugin_post(
            token,
            "getAdvanceLogs",
            {"jobId": job_id, "spaceKey": self.client.config.spacekey},
            user_id=user_id,
            docid=job_docid(job_id),
        )

    def verify_job(self, token: str, job_id: str, user_id: str = "") -> dict[str, Any]:
        job = self.get_job_by_id(token, job_id, user_id)
        workflow = self.get_job_workflow(token, job_id, user_id)
        workflow_found = isinstance(workflow, dict) and workflow.get("success") is True
        return {
            "resourceType": "job",
            "jobId": job_id,
            "jobFound": bool(job),
            "workflowFound": workflow_found,
            "job": job,
            "workflow": workflow,
        }

