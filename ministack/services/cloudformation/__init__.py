"""
CloudFormation Service Emulator -- AWS-compatible.
Supports: CreateStack, UpdateStack, DeleteStack, DescribeStacks, ListStacks,
          DescribeStackEvents, DescribeStackResource, DescribeStackResources,
          ListStackResources, GetTemplate, ValidateTemplate, ListExports,
          CreateChangeSet, DescribeChangeSet, ExecuteChangeSet,
          DeleteChangeSet, ListChangeSets,
          GetTemplateSummary.
Uses Query API (Action=...) with form-encoded body.
"""

import json
import logging
import os
from urllib.parse import parse_qs

from ministack.core.responses import AccountScopedDict

logger = logging.getLogger("cloudformation")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# In-memory state (shared across all submodules)
_stacks = AccountScopedDict()          # stack_name -> stack dict
_stack_events = AccountScopedDict()    # stack_id -> [event list]
_exports = AccountScopedDict()         # export_name -> {StackId, Name, Value}
_change_sets = AccountScopedDict()     # cs_id -> change set dict

# Re-exports for compatibility
from .engine import (  # noqa: E402
    _NO_VALUE,
    _evaluate_conditions,
    _extract_deps,
    _parse_template,
    _resolve_parameters,
    _resolve_refs,
    _topological_sort,
)
from .helpers import _p  # noqa: E402


async def handle_request(method: str, path: str, headers: dict,
                         body: bytes, query_params: dict) -> tuple:
    params = dict(query_params)
    content_type = headers.get("content-type", "")
    target = headers.get("x-amz-target", "")

    # JSON protocol (newer SDKs): X-Amz-Target: CloudFormation_20100515.ActionName
    if "amz-json" in content_type and target.startswith("CloudFormation_20100515."):
        action_name = target.split(".")[-1]
        params["Action"] = [action_name]
        if body:
            try:
                json_body = json.loads(body)
                for k, v in json_body.items():
                    params[k] = [str(v)] if not isinstance(v, list) else v
            except (json.JSONDecodeError, TypeError):
                pass
    elif method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")
    handler = _ACTION_HANDLERS.get(action)
    if not handler:
        from .helpers import _error
        return _error("InvalidAction", f"Unknown action: {action}", 400)
    return handler(params)


def reset():
    _stacks.clear()
    _stack_events.clear()
    _exports.clear()
    _change_sets.clear()
    from ministack.services.cloudformation import custom_resource as _cr
    _cr.reset()


def get_state():
    """Snapshot CFN bookkeeping for persistence.

    Stack dicts carry callables and compiled regexes in some edge cases; we
    drop the parsed template (re-parsed on demand from _template_body) and
    the resolved-conditions cache (re-evaluated). Everything else is JSON-
    serialisable: template body strings, parameter dicts, physical-id maps.
    """
    import copy

    def _clean_stack(s):
        c = {k: v for k, v in s.items() if k not in ("_template", "_conditions")}
        return copy.deepcopy(c)

    stacks_clean = AccountScopedDict()
    for k, v in _stacks._data.items():
        stacks_clean._data[k] = _clean_stack(v) if isinstance(v, dict) else v

    return {
        "stacks": stacks_clean,
        "stack_events": copy.deepcopy(_stack_events),
        "exports": copy.deepcopy(_exports),
        "change_sets": copy.deepcopy(_change_sets),
    }


# Non-terminal statuses cannot recover on their own after a restart: state is
# persisted only on graceful shutdown (app.py lifespan.shutdown), and the
# background _deploy_stack_async task that flips the status to terminal lives
# only in the previous process's event loop. Any *_IN_PROGRESS we reload is
# therefore orphaned. Map each to the nearest terminal status so the next deploy
# isn't permanently rejected by UpdateStack (handlers._update_stack). Resources
# are provisioned incrementally and may be left partially applied if the process
# died mid-loop, but UPDATE_IN_PROGRESS still resolves optimistically to
# UPDATE_COMPLETE: a recoverable (if possibly drifted) stack beats a permanently
# wedged one, and the next UpdateStack reconciles any drift. REVIEW_IN_PROGRESS
# is excluded: it is already recoverable via a fresh CreateChangeSet.
_ORPHANED_STATUS_RECOVERY = {
    "CREATE_IN_PROGRESS": "CREATE_FAILED",
    "UPDATE_IN_PROGRESS": "UPDATE_COMPLETE",
    "UPDATE_ROLLBACK_IN_PROGRESS": "UPDATE_ROLLBACK_COMPLETE",
    "ROLLBACK_IN_PROGRESS": "ROLLBACK_COMPLETE",
    "DELETE_IN_PROGRESS": "DELETE_COMPLETE",
}


def restore_state(data):
    if not data:
        return
    if isinstance(data.get("stacks"), AccountScopedDict):
        _stacks._data.update(data["stacks"]._data)
    if isinstance(data.get("stack_events"), AccountScopedDict):
        _stack_events._data.update(data["stack_events"]._data)
    if isinstance(data.get("exports"), AccountScopedDict):
        _exports._data.update(data["exports"]._data)
    if isinstance(data.get("change_sets"), AccountScopedDict):
        _change_sets._data.update(data["change_sets"]._data)

    # Single pass over the raw _data so EVERY account is covered: at import time
    # the account context is the default 000000000000, but deployed stacks live
    # under other accounts, and .values()/.items() would skip them entirely.
    for stack in _stacks._data.values():
        if not isinstance(stack, dict):
            continue

        # Re-parse templates so downstream describe/update/delete calls work.
        if stack.get("_template_body") and not stack.get("_template"):
            try:
                stack["_template"] = _parse_template(stack["_template_body"])
                stack["_conditions"] = _evaluate_conditions(
                    stack["_template"], stack.get("_resolved_params", {})
                )
            except Exception:
                logger.exception("Failed to re-parse template for %s on restore", stack.get("StackName"))

        # Reconcile an orphaned in-progress status left by a restart mid-deploy:
        # the driving _deploy_stack_async task did not survive the restart, so
        # the status would otherwise dangle forever.
        current = stack.get("StackStatus", "")
        recovered = _ORPHANED_STATUS_RECOVERY.get(current)
        if recovered:
            stack["StackStatus"] = recovered
            stack["StackStatusReason"] = (
                f"ministack: recovered orphaned {current} on restart "
                "(deploy task did not survive process restart)"
            )
            logger.warning("Reconciled orphaned CFN stack %s: %s -> %s",
                           stack.get("StackName", "?"), current, recovered)


# Auto-load persisted state at module import time, matching the pattern used
# by other persistent services (sqs, kms, etc).
try:
    from ministack.core.persistence import load_state as _load_state
    _restored = _load_state("cloudformation")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted cloudformation state")


# Must be last — handlers imports from this module
from ministack.core.responses import AccountScopedDict, get_account_id

from .handlers import _ACTION_HANDLERS, _validate_template  # noqa: E402
