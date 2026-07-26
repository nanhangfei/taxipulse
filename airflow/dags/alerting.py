"""Shared alerting helpers.

Kept in its own module (not a DAG file) so DAGs can reuse ``slack_alert``
without importing each other — importing a DAG-defining module re-instantiates
its DAG and trips AirflowDagDuplicatedIdException.
"""
from __future__ import annotations

import json
import os
import urllib.request


def slack_alert(context) -> None:
    """Minimal Slack alerting — reads webhook from env, never from code."""
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return
    ti = context["task_instance"]
    msg = {
        "text": (f":red_circle: *{ti.dag_id}.{ti.task_id}* failed "
                 f"(run {context['ds']})\n<{ti.log_url}|logs>")
    }
    req = urllib.request.Request(
        url, data=json.dumps(msg).encode(), headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=10)
