"""
CodePipeline Service Emulator.
JSON-based API via X-Amz-Target: CodePipeline_20150709.<Operation>.

Tier 1 (this file): full CRUD for pipelines, immediate-Succeeded executions.
Tier 2 (codepipeline_runtime.py): real stage/action orchestration (registered via
set_executor()).
"""

import asyncio
import copy
import json
import logging
import os
import time

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("codepipeline")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

_pipelines = AccountScopedDict()        # pipeline_name -> pipeline definition
_executions = AccountScopedDict()       # execution_id -> execution record
_webhooks = AccountScopedDict()         # webhook_name -> webhook definition
_tags = AccountScopedDict()             # resource_arn -> {tag_key: tag_value}

# Executor hook — Tier 2 replaces this. Until then, executions auto-succeed.
_executor = None


def set_executor(fn):
    """Register a coroutine fn(pipeline_def, execution_record) -> None."""
    global _executor
    _executor = fn


def reset():
    _pipelines.clear()
    _executions.clear()
    _webhooks.clear()
    _tags.clear()


def get_state():
    return copy.deepcopy({
        "pipelines": _pipelines,
        "executions": _executions,
        "webhooks": _webhooks,
        "tags": _tags,
    })


def restore_state(data):
    _pipelines.update(data.get("pipelines", {}))
    _executions.update(data.get("executions", {}))
    _webhooks.update(data.get("webhooks", {}))
    _tags.update(data.get("tags", {}))


try:
    _restored = load_state("codepipeline")
    if _restored:
        restore_state(_restored)
except Exception:
    logging.getLogger(__name__).exception(
        "Failed to restore persisted codepipeline state; continuing with fresh store"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pipeline_arn(name):
    return f"arn:aws:codepipeline:{get_region()}:{get_account_id()}:{name}"


def _now_ts():
    """awsjson1.0 timestamps are epoch-seconds floats. JS SDK v3 will NaN on
    ISO strings; boto3 accepts either but we standardise on floats."""
    return time.time()


def _now_iso():
    """Kept as an alias for code paths still passing strings."""
    return _now_ts()


def _ts(value):
    """Coerce any stored timestamp (legacy ISO string or epoch number) to a
    float for the response wire format. Returns 0.0 for None / unparseable."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value:
        try:
            return float(value)
        except ValueError:
            # Legacy ISO string from before the float switch.
            import datetime
            try:
                stripped = value.rstrip("Z")
                dt = datetime.datetime.fromisoformat(stripped)
                return dt.replace(tzinfo=datetime.timezone.utc).timestamp()
            except ValueError:
                return 0.0
    return 0.0


def _make_execution_record(pipeline_name, pipeline_def, status="Succeeded", trigger=None):
    execution_id = new_uuid()
    now = _now_iso()
    return {
        "pipelineExecutionId": execution_id,
        "pipelineName": pipeline_name,
        "pipelineVersion": pipeline_def.get("version", 1),
        "status": status,
        "statusSummary": status,
        "startTime": now,
        "lastUpdateTime": now,
        "trigger": trigger or {"triggerType": "StartPipelineExecution", "triggerDetail": "ministack"},
        "artifactRevisions": [],
        "stageStates": _build_initial_stage_states(pipeline_def, status),
    }


def _ck(d, *keys):
    """Get the first matching key, case-insensitive on first char."""
    for k in keys:
        if k in d:
            return d[k]
    return None


def _build_initial_stage_states(pipeline_def, status):
    # A run that's about to be driven action-by-action (status "InProgress")
    # must NOT pre-stamp every action as InProgress: actions behind a paused
    # approval gate would otherwise show a perpetual spinner because the
    # runtime never reaches them. Seed those as not-started (no latestExecution)
    # so the UI shows them pending until the runtime flips each to InProgress as
    # it starts. Terminal seeds (status "Succeeded": the Tier-1 stub executor
    # and the GetPipelineState no-execution fallback) record the status on every
    # action so the whole pipeline renders complete.
    pending = status == "InProgress"
    states = []
    for stage in _ck(pipeline_def, "stages", "Stages") or []:
        action_states = []
        for action in _ck(stage, "actions", "Actions") or []:
            entry = {
                "actionName": _ck(action, "name", "Name") or "",
                "currentRevision": {},
            }
            if not pending:
                entry["latestExecution"] = {
                    "status": status,
                    "summary": f"{status} (ministack)",
                    "lastStatusChange": _now_iso(),
                }
            action_states.append(entry)
        stage_entry = {
            "stageName": _ck(stage, "name", "Name") or "",
            "inboundTransitionState": {"enabled": True},
            "actionStates": action_states,
        }
        if not pending:
            stage_entry["latestExecution"] = {"pipelineExecutionId": "", "status": status}
        states.append(stage_entry)
    return states


async def _default_executor(pipeline_def, execution_record):
    """Tier 1 default: do nothing. Execution is already Succeeded."""
    return None


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreatePipeline": _create_pipeline,
        "GetPipeline": _get_pipeline,
        "UpdatePipeline": _update_pipeline,
        "DeletePipeline": _delete_pipeline,
        "ListPipelines": _list_pipelines,
        "GetPipelineState": _get_pipeline_state,
        "StartPipelineExecution": _start_pipeline_execution,
        "StopPipelineExecution": _stop_pipeline_execution,
        "GetPipelineExecution": _get_pipeline_execution,
        "ListPipelineExecutions": _list_pipeline_executions,
        "ListActionExecutions": _list_action_executions,
        "ListActionTypes": _list_action_types,
        "RetryStageExecution": _retry_stage_execution,
        "PutWebhook": _put_webhook,
        "DeleteWebhook": _delete_webhook,
        "ListWebhooks": _list_webhooks,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "PollForJobs": lambda d: json_response({"jobs": []}),
        "AcknowledgeJob": lambda d: json_response({"status": "InProgress"}),
        "PutJobSuccessResult": lambda d: json_response({}),
        "PutJobFailureResult": lambda d: json_response({}),
        "PollForThirdPartyJobs": lambda d: json_response({"jobs": []}),
        "AcknowledgeThirdPartyJob": lambda d: json_response({"status": "InProgress"}),
        "PutThirdPartyJobSuccessResult": lambda d: json_response({}),
        "PutThirdPartyJobFailureResult": lambda d: json_response({}),
        "RegisterWebhookWithThirdParty": lambda d: json_response({}),
        "DeregisterWebhookWithThirdParty": lambda d: json_response({}),
        "PutApprovalResult": _put_approval_result,
        "EnableStageTransition": lambda d: json_response({}),
        "DisableStageTransition": lambda d: json_response({}),
        "PutActionRevision": lambda d: json_response({"newRevision": False, "pipelineExecutionId": ""}),
    }

    handler = handlers.get(action)
    if not handler:
        logger.warning("Unknown CodePipeline action: %s", action)
        return error_response_json("InvalidActionException", f"Unknown action: {action}", 400)

    result = handler(data)
    if asyncio.iscoroutine(result):
        result = await result
    return result


# ---------------------------------------------------------------------------
# Pipeline CRUD
# ---------------------------------------------------------------------------

def _create_pipeline(data):
    pipeline = data.get("pipeline", {})
    name = pipeline.get("name", "")
    if not name:
        return error_response_json("ValidationException", "Pipeline name is required", 400)
    if name in _pipelines:
        return error_response_json(
            "PipelineNameInUseException", f"Pipeline already exists: {name}", 400
        )
    record = copy.deepcopy(pipeline)
    record["version"] = 1
    record["created"] = _now_iso()
    record["updated"] = record["created"]
    record.setdefault("roleArn", f"arn:aws:iam::{get_account_id()}:role/codepipeline-role")
    _pipelines[name] = record
    arn = _pipeline_arn(name)
    tags = data.get("tags", []) or []
    if tags:
        _tags[arn] = {t["key"]: t["value"] for t in tags}
    logger.info("CreatePipeline: %s", name)
    return json_response({"pipeline": copy.deepcopy(record), "tags": tags})


def _get_pipeline(data):
    name = data.get("name", "")
    pipeline = _pipelines.get(name)
    if not pipeline:
        return error_response_json(
            "PipelineNotFoundException", f"Pipeline not found: {name}", 400
        )
    metadata = {
        "pipelineArn": _pipeline_arn(name),
        "created": _ts(pipeline.get("created", _now_iso())),
        "updated": _ts(pipeline.get("updated", _now_iso())),
    }
    return json_response({"pipeline": copy.deepcopy(pipeline), "metadata": metadata})


def _update_pipeline(data):
    pipeline = data.get("pipeline", {})
    name = pipeline.get("name", "")
    existing = _pipelines.get(name)
    if not existing:
        return error_response_json(
            "PipelineNotFoundException", f"Pipeline not found: {name}", 400
        )
    record = copy.deepcopy(pipeline)
    record["version"] = existing.get("version", 1) + 1
    record["created"] = existing.get("created", _now_iso())
    record["updated"] = _now_iso()
    record.setdefault("roleArn", existing.get("roleArn"))
    _pipelines[name] = record
    logger.info("UpdatePipeline: %s (v%d)", name, record["version"])
    return json_response({"pipeline": copy.deepcopy(record)})


def _delete_pipeline(data):
    name = data.get("name", "")
    _pipelines.pop(name, None)
    for eid in [e for e, x in _executions.items() if x.get("pipelineName") == name]:
        _executions.pop(eid, None)
    logger.info("DeletePipeline: %s", name)
    return json_response({})


def _list_pipelines(data):
    items = []
    for name, p in _pipelines.items():
        items.append({
            "name": name,
            "version": p.get("version", 1),
            "created": _ts(p.get("created", _now_iso())),
            "updated": _ts(p.get("updated", _now_iso())),
            "pipelineType": p.get("pipelineType", "V1"),
            "executionMode": p.get("executionMode", "SUPERSEDED"),
        })
    return json_response({"pipelines": items})


def _get_pipeline_state(data):
    name = data.get("name", "")
    pipeline = _pipelines.get(name)
    if not pipeline:
        return error_response_json(
            "PipelineNotFoundException", f"Pipeline not found: {name}", 400
        )
    latest = None
    for execution in _executions.values():
        if execution.get("pipelineName") != name:
            continue
        if latest is None or execution.get("startTime", "") > latest.get("startTime", ""):
            latest = execution
    if latest is None:
        stage_states = _build_initial_stage_states(pipeline, "Succeeded")
    else:
        stage_states = latest.get("stageStates", _build_initial_stage_states(pipeline, "Succeeded"))
    return json_response({
        "pipelineName": name,
        "pipelineVersion": pipeline.get("version", 1),
        "stageStates": stage_states,
        "created": _ts(pipeline.get("created", _now_iso())),
        "updated": _ts(pipeline.get("updated", _now_iso())),
    })


# ---------------------------------------------------------------------------
# Pipeline executions
# ---------------------------------------------------------------------------

async def _start_pipeline_execution(data):
    name = data.get("name", "")
    pipeline = _pipelines.get(name)
    if not pipeline:
        return error_response_json(
            "PipelineNotFoundException", f"Pipeline not found: {name}", 400
        )
    executor = _executor or _default_executor
    if executor is _default_executor:
        execution = _make_execution_record(name, pipeline, status="Succeeded")
        _executions[execution["pipelineExecutionId"]] = execution
        logger.info("StartPipelineExecution (stub): %s -> %s", name, execution["pipelineExecutionId"])
        return json_response({"pipelineExecutionId": execution["pipelineExecutionId"]})

    execution = _make_execution_record(name, pipeline, status="InProgress")
    _executions[execution["pipelineExecutionId"]] = execution
    logger.info("StartPipelineExecution (real): %s -> %s", name, execution["pipelineExecutionId"])

    async def _runner():
        try:
            await executor(pipeline, execution)
        except Exception:
            logger.exception("Pipeline execution failed: %s", execution["pipelineExecutionId"])
            execution["status"] = "Failed"
            execution["statusSummary"] = "Executor raised"
            execution["lastUpdateTime"] = _now_iso()

    asyncio.create_task(_runner())
    return json_response({"pipelineExecutionId": execution["pipelineExecutionId"]})


def _stop_pipeline_execution(data):
    eid = data.get("pipelineExecutionId", "")
    execution = _executions.get(eid)
    if not execution:
        return error_response_json(
            "PipelineExecutionNotFoundException", f"Execution not found: {eid}", 400
        )
    execution["status"] = "Stopped"
    execution["lastUpdateTime"] = _now_iso()
    return json_response({"pipelineExecutionId": eid})


def _put_approval_result(data):
    """Resolve a manual-approval gate the runtime paused at. result.status
    Approved → resume the run (deploy stages after the gate); Rejected → fail.
    """
    pname = data.get("pipelineName", "")
    stage = data.get("stageName", "")
    action = data.get("actionName", "")
    result = data.get("result", {}) or {}
    approved = result.get("status") == "Approved"
    summary = result.get("summary", "")
    # Lazy import: codepipeline_runtime imports this module at load time.
    from ministack.services import codepipeline_runtime as _rt
    eid = _rt.find_paused(pname, stage, action)
    if not eid:
        return error_response_json(
            "ApprovalAlreadyCompletedException",
            f"No pending approval for {pname}/{stage}/{action}", 400,
        )
    _rt.resume_paused(eid, approved, summary)
    return json_response({"approvedAt": _now_iso()})


def _get_pipeline_execution(data):
    eid = data.get("pipelineExecutionId", "")
    pname = data.get("pipelineName", "")
    execution = _executions.get(eid)
    if not execution or (pname and execution.get("pipelineName") != pname):
        return error_response_json(
            "PipelineExecutionNotFoundException", f"Execution not found: {eid}", 400
        )
    return json_response({
        "pipelineExecution": {
            "pipelineName": execution["pipelineName"],
            "pipelineVersion": execution.get("pipelineVersion", 1),
            "pipelineExecutionId": execution["pipelineExecutionId"],
            "status": execution["status"],
            "statusSummary": execution.get("statusSummary", execution["status"]),
            "artifactRevisions": execution.get("artifactRevisions", []),
            "trigger": execution.get("trigger", {}),
        }
    })


def _list_pipeline_executions(data):
    name = data.get("pipelineName", "")
    summaries = []
    for execution in _executions.values():
        if execution.get("pipelineName") != name:
            continue
        summaries.append({
            "pipelineExecutionId": execution["pipelineExecutionId"],
            "status": execution["status"],
            "startTime": _ts(execution.get("startTime")),
            "lastUpdateTime": _ts(execution.get("lastUpdateTime")),
            "sourceRevisions": [],
            "trigger": execution.get("trigger", {}),
        })
    summaries.sort(key=lambda s: s.get("startTime", ""), reverse=True)
    return json_response({"pipelineExecutionSummaries": summaries})


def _list_action_executions(data):
    name = data.get("pipelineName", "")
    out = []
    for execution in _executions.values():
        if execution.get("pipelineName") != name:
            continue
        for stage in execution.get("stageStates", []):
            for action_state in stage.get("actionStates", []):
                out.append({
                    "pipelineExecutionId": execution["pipelineExecutionId"],
                    "actionExecutionId": new_uuid(),
                    "pipelineVersion": execution.get("pipelineVersion", 1),
                    "stageName": stage["stageName"],
                    "actionName": action_state["actionName"],
                    "startTime": _ts(execution.get("startTime")),
                    "lastUpdateTime": _ts(action_state.get("latestExecution", {}).get("lastStatusChange")),
                    "status": action_state.get("latestExecution", {}).get("status", "Succeeded"),
                    "input": {},
                    "output": {},
                })
    return json_response({"actionExecutionDetails": out})


def _retry_stage_execution(data):
    return json_response({"pipelineExecutionId": data.get("pipelineExecutionId", "")})


# ---------------------------------------------------------------------------
# Action types — CDK Pipelines reads these during synth fingerprint validation.
# ---------------------------------------------------------------------------

_STANDARD_ACTION_TYPES = [
    ("Source", "S3", "AWS"), ("Source", "CodeStarSourceConnection", "AWS"),
    ("Source", "CodeCommit", "AWS"), ("Source", "ECR", "AWS"), ("Source", "GitHub", "ThirdParty"),
    ("Build", "CodeBuild", "AWS"), ("Build", "Jenkins", "Custom"),
    ("Deploy", "CloudFormation", "AWS"), ("Deploy", "CodeDeploy", "AWS"),
    ("Deploy", "S3", "AWS"), ("Deploy", "ECS", "AWS"), ("Deploy", "OpsWorks", "AWS"),
    ("Test", "CodeBuild", "AWS"), ("Test", "Jenkins", "Custom"),
    ("Approval", "Manual", "AWS"),
    ("Invoke", "Lambda", "AWS"), ("Invoke", "StepFunctions", "AWS"),
]


def _list_action_types(data):
    out = []
    for category, provider, owner in _STANDARD_ACTION_TYPES:
        out.append({
            "id": {"category": category, "owner": owner, "provider": provider, "version": "1"},
            "inputArtifactDetails": {"minimumCount": 0, "maximumCount": 5},
            "outputArtifactDetails": {"minimumCount": 0, "maximumCount": 5},
            "settings": {},
        })
    return json_response({"actionTypes": out})


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------

def _put_webhook(data):
    webhook = data.get("webhook", {})
    name = webhook.get("name", "")
    if not name:
        return error_response_json("ValidationException", "Webhook name required", 400)
    _webhooks[name] = webhook
    return json_response({"webhook": {
        "definition": webhook,
        "url": f"http://localhost:4566/_ministack/codepipeline/webhook/{name}",
        "errorMessage": "",
        "errorCode": "",
        "lastTriggered": 0.0,
        "arn": f"arn:aws:codepipeline:{get_region()}:{get_account_id()}:webhook:{name}",
        "tags": [],
    }})


def _delete_webhook(data):
    _webhooks.pop(data.get("name", ""), None)
    return json_response({})


def _list_webhooks(data):
    out = []
    for name, webhook in _webhooks.items():
        out.append({
            "definition": webhook,
            "url": f"http://localhost:4566/_ministack/codepipeline/webhook/{name}",
            "lastTriggered": 0.0,
            "arn": f"arn:aws:codepipeline:{get_region()}:{get_account_id()}:webhook:{name}",
            "tags": [],
        })
    return json_response({"webhooks": out})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("resourceArn", "")
    current = _tags.get(arn, {})
    for t in data.get("tags", []) or []:
        current[t["key"]] = t["value"]
    _tags[arn] = current
    return json_response({})


def _untag_resource(data):
    arn = data.get("resourceArn", "")
    current = _tags.get(arn, {})
    for k in data.get("tagKeys", []) or []:
        current.pop(k, None)
    _tags[arn] = current
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("resourceArn", "")
    return json_response({
        "tags": [{"key": k, "value": v} for k, v in _tags.get(arn, {}).items()],
        "nextToken": None,
    })
