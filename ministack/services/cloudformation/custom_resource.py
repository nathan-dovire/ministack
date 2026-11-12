"""
CloudFormation Custom Resource — ResponseURL intercept and Lambda invocation.
"""
import json
import logging
import os
import threading

logger = logging.getLogger("cloudformation")

_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_PORT = os.environ.get("GATEWAY_PORT", "4566")

# A custom-resource Lambda that never PUTs back to its ResponseURL holds the
# whole stack in *_IN_PROGRESS for this long. Real CFN defaults to 1h; locally
# that just widens the window for a restart to persist a dangling IN_PROGRESS
# and permanently wedge the stack (see cloudformation.restore_state). Fail fast
# instead: a terminal *_FAILED/rollback is recoverable, a persisted
# IN_PROGRESS is not. 300s keeps headroom for a legitimately slow resource
# while staying 12x below the old 1h. Override with
# MINISTACK_CUSTOM_RESOURCE_TIMEOUT or a per-resource ServiceTimeout property.
_DEFAULT_SERVICE_TIMEOUT = int(os.environ.get("MINISTACK_CUSTOM_RESOURCE_TIMEOUT", "300"))


def _synthetic_success() -> bool:
    return os.environ.get("MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS", "0").lower() in ("1", "true", "yes")


def _mock_data() -> dict:
    raw = os.environ.get("MINISTACK_CUSTOM_RESOURCE_MOCK_DATA", "")
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


_lock = threading.Lock()
# token → {"event": threading.Event, "result": dict | None}
_pending: dict = {}


def register_token(token: str) -> threading.Event:
    evt = threading.Event()
    with _lock:
        _pending[token] = {"event": evt, "result": None}
    return evt


_SDK_NOT_FOUND_PATTERNS = (
    "No client constructor found",
    "Cannot find module",
    "is not a constructor",
    "Cannot find package",
    "MODULE_NOT_FOUND",
)


def deliver_response(token: str, response: dict) -> bool:
    """Called by the HTTP handler when ResponseURL is PUT to. Returns False if token unknown.

    MiniStack-specific fixup, only with MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS=1:
    when a CDK AwsCustomResource framework Lambda
    reports FAILED because Node's bundled handler can't find the AWS SDK
    package it needs at runtime (CDK bundles with --external:aws-sdk,
    expecting the real Lambda runtime to provide it), we rewrite the response
    to SUCCESS. The Lambda would have returned data the dependent resources
    need; with no real implementation we synthesize a minimal Data dict from
    the original event's ResourceProperties to keep Fn::GetAtt lookups
    populated. Real downstream behavior may be wrong, but CFN provisioning
    completes instead of hanging the whole stack.
    """
    with _lock:
        entry = _pending.get(token)
        if entry is None:
            return False
        if response.get("Status") == "FAILED" and _synthetic_success():
            reason = response.get("Reason", "")
            if any(pat in reason for pat in _SDK_NOT_FOUND_PATTERNS):
                logger.warning(
                    "Custom resource failure rewritten to SUCCESS (token=%s): %s",
                    token, reason,
                )
                response = {
                    **response,
                    "Status": "SUCCESS",
                    "Reason": f"ministack: {reason} — rewritten to SUCCESS",
                    "Data": response.get("Data") or _mock_data(),
                }
        entry["result"] = response
        entry["event"].set()
    return True


def reset():
    with _lock:
        _pending.clear()


def _response_url(token: str) -> str:
    return f"http://{_HOST}:{_PORT}/_ministack/cfn-response/{token}"


def _func_name_from_arn(service_token: str) -> str:
    """Extract function name from a Lambda ARN, or return as-is."""
    if service_token.startswith("arn:"):
        parts = service_token.split(":")
        # arn:aws:lambda:region:account:function:<name>[:<qualifier>]
        return parts[6] if len(parts) >= 7 else parts[-1]
    return service_token


def invoke_custom_resource(
    request_type: str,
    logical_id: str,
    props: dict,
    stack_name: str,
    stack_id: str,
    resource_type: str,
    physical_id: str | None = None,
    old_props: dict | None = None,
) -> tuple:
    """Invoke the ServiceToken Lambda and block until it PUTs to the ResponseURL.

    CALLER CONTRACT: Must be run in a worker thread (via asyncio.to_thread), never
    directly from a coroutine — it blocks on threading.Event.wait() so the event
    loop must remain free to receive the ResponseURL PUT callback.

    Returns (physical_resource_id, data_attributes).
    Raises ValueError if the Lambda is not found.
    Raises TimeoutError if no response arrives within ServiceTimeout seconds.
    Raises RuntimeError if the Lambda responds with Status=FAILED.
    """
    import ministack.services.lambda_svc as _lambda_svc
    from ministack.core.responses import new_uuid

    service_token = props.get("ServiceToken", "")
    func_name = _func_name_from_arn(service_token)

    if func_name not in _lambda_svc._functions:
        raise ValueError(
            f"Custom resource ServiceToken {service_token!r} not found. "
            "Ensure the Lambda function is provisioned before the custom resource."
        )

    func_record = _lambda_svc._functions[func_name]
    try:
        service_timeout = int(props.get("ServiceTimeout", _DEFAULT_SERVICE_TIMEOUT))
    except (ValueError, TypeError):
        service_timeout = _DEFAULT_SERVICE_TIMEOUT

    token = new_uuid()
    request_id = new_uuid()

    cfn_event = {
        "RequestType": request_type,
        "RequestId": request_id,
        "StackId": stack_id,
        "ResponseURL": _response_url(token),
        "ResourceType": resource_type,
        "LogicalResourceId": logical_id,
        "ResourceProperties": dict(props),
    }
    if physical_id is not None:
        cfn_event["PhysicalResourceId"] = physical_id
    if old_props is not None:
        cfn_event["OldResourceProperties"] = dict(old_props)

    event_obj = register_token(token)

    try:
        _lambda_svc._execute_function(func_record, cfn_event)
    except Exception as exc:
        logger.warning("Custom resource Lambda raised synchronously: %s", exc)

    signalled = event_obj.wait(timeout=service_timeout)

    with _lock:
        entry = _pending.pop(token, None)

    if not signalled or entry is None or entry["result"] is None:
        raise TimeoutError(
            f"Custom resource {logical_id!r} timed out after {service_timeout}s "
            "waiting for ResponseURL callback"
        )

    result = entry["result"]
    if result.get("Status") == "FAILED":
        raise RuntimeError(result.get("Reason", "Custom resource reported FAILED"))

    pid = result.get("PhysicalResourceId") or (physical_id if physical_id else request_id)
    return pid, result.get("Data") or {}
