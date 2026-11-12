"""
CodeStar Connections Service Emulator.
JSON-based API via X-Amz-Target: com.amazonaws.codestar.connections.CodeStar_connections_20191201.<Operation>.

Stores connection records and an optional `local_source_path` mapping that the
CodePipeline source action uses to resolve a Bitbucket/GitHub source to a
filesystem path. Bootstrapping a connection that points at a local working
tree (`MINISTACK_CONNECTION_<name>=/path/to/repo`) means pipeline executions
don't need network access.
"""

import copy
import json
import logging
import os
import time

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("codestar-connections")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_connections = AccountScopedDict()    # connection_id -> connection record
_hosts = AccountScopedDict()          # host_id -> host record
_tags = AccountScopedDict()           # resource_arn -> {tag_key: tag_value}


def reset():
    _connections.clear()
    _hosts.clear()
    _tags.clear()


def get_state():
    return copy.deepcopy({"connections": _connections, "hosts": _hosts, "tags": _tags})


def restore_state(data):
    _connections.update(data.get("connections", {}))
    _hosts.update(data.get("hosts", {}))
    _tags.update(data.get("tags", {}))


try:
    _restored = load_state("codestar_connections")
    if _restored:
        restore_state(_restored)
except Exception:
    logging.getLogger(__name__).exception(
        "Failed to restore persisted codestar-connections state; continuing fresh"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connection_arn(conn_id):
    return f"arn:aws:codestar-connections:{get_region()}:{get_account_id()}:connection/{conn_id}"


def _host_arn(host_id):
    return f"arn:aws:codestar-connections:{get_region()}:{get_account_id()}:host/{host_id}"


def _resolve_local_source_path(connection_name):
    """Look up MINISTACK_CONNECTION_<NAME> env var for a local source path.

    Connection names may contain hyphens / dots; env vars are uppercased and
    non-alnum chars are replaced with underscores.
    """
    if not connection_name:
        return None
    safe = "".join(c.upper() if c.isalnum() else "_" for c in connection_name)
    return os.environ.get(f"MINISTACK_CONNECTION_{safe}")


def lookup_connection_by_arn(arn):
    """Public helper used by the CodePipeline source action."""
    if not arn or "connection/" not in arn:
        return None
    conn_id = arn.rsplit("/", 1)[-1]
    return _connections.get(conn_id)


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
        "CreateConnection": _create_connection,
        "GetConnection": _get_connection,
        "DeleteConnection": _delete_connection,
        "ListConnections": _list_connections,
        "CreateHost": _create_host,
        "GetHost": _get_host,
        "DeleteHost": _delete_host,
        "ListHosts": _list_hosts,
        "UpdateHost": _update_host,
        "UpdateConnectionInstallation": lambda d: json_response({}),
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "GetRepositoryLink": lambda d: error_response_json("ResourceNotFoundException", "no link", 404),
        "ListRepositoryLinks": lambda d: json_response({"repositoryLinks": []}),
    }

    handler = handlers.get(action)
    if not handler:
        logger.warning("Unknown CodeStar Connections action: %s", action)
        return error_response_json("InvalidActionException", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def _create_connection(data):
    name = data.get("ConnectionName") or data.get("connectionName") or ""
    if not name:
        return error_response_json("ValidationException", "ConnectionName is required", 400)
    host_arn = data.get("HostArn") or data.get("hostArn") or ""
    provider = data.get("ProviderType") or data.get("providerType") or ""
    if not provider and "host/" in host_arn:
        provider = (_hosts.get(host_arn.rsplit("/", 1)[-1]) or {}).get("ProviderType", "")
    if not provider:
        return error_response_json("ValidationException", "ProviderType or HostArn is required", 400)

    conn_id = new_uuid()
    arn = _connection_arn(conn_id)
    record = {
        "ConnectionArn": arn,
        "ConnectionName": name,
        "ProviderType": provider,
        "HostArn": host_arn,
        "OwnerAccountId": get_account_id(),
        # AWS returns AVAILABLE only after OAuth completion; for local we skip
        # the handshake and mark available immediately so CDK Pipelines
        # validators don't block waiting.
        "ConnectionStatus": "AVAILABLE",
        "LocalSourcePath": _resolve_local_source_path(name),
    }
    _connections[conn_id] = record
    tags = data.get("Tags") or data.get("tags") or []
    if tags:
        _tags[arn] = {t["Key"]: t["Value"] for t in tags}
    logger.info("CreateConnection: %s -> %s (local_path=%s)", name, arn,
                record["LocalSourcePath"])
    return json_response({"ConnectionArn": arn, "Tags": tags})


def _get_connection(data):
    arn = data.get("ConnectionArn") or data.get("connectionArn") or ""
    record = lookup_connection_by_arn(arn)
    if not record:
        # Auto-provision an "available" connection if the ARN matches our
        # account/region. Lets CDK pipelines that reference a CodeStar
        # connection ARN baked into their templates "just work" against
        # ministack without manual setup, with the local path auto-resolved
        # from the env if present.
        if arn and "connection/" in arn:
            conn_id = arn.rsplit("/", 1)[-1]
            record = {
                "ConnectionArn": arn,
                "ConnectionName": f"auto-{conn_id[:8]}",
                "HostArn": "",
                "OwnerAccountId": get_account_id(),
                "ConnectionStatus": "AVAILABLE",
                "LocalSourcePath": os.environ.get("MINISTACK_CONNECTION_DEFAULT_PATH"),
            }
            _connections[conn_id] = record
        else:
            return error_response_json(
                "ResourceNotFoundException", f"Connection not found: {arn}", 400
            )
    return json_response({"Connection": _connection_shape(record)})


def _delete_connection(data):
    arn = data.get("ConnectionArn") or data.get("connectionArn") or ""
    if arn and "connection/" in arn:
        conn_id = arn.rsplit("/", 1)[-1]
        _connections.pop(conn_id, None)
    return json_response({})


def _list_connections(data):
    items = [_connection_shape(c) for c in _connections.values()]
    return json_response({"Connections": items, "NextToken": None})


def _connection_shape(record):
    return {
        "ConnectionName": record.get("ConnectionName", ""),
        "ConnectionArn": record.get("ConnectionArn", ""),
        "ProviderType": record.get("ProviderType", ""),
        "OwnerAccountId": record.get("OwnerAccountId", get_account_id()),
        "ConnectionStatus": record.get("ConnectionStatus", "AVAILABLE"),
        "HostArn": record.get("HostArn", ""),
    }


# ---------------------------------------------------------------------------
# Hosts
# ---------------------------------------------------------------------------

def _create_host(data):
    name = data.get("Name") or data.get("name") or ""
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    provider = data.get("ProviderType") or data.get("providerType") or "GitHubEnterpriseServer"
    endpoint = data.get("ProviderEndpoint") or data.get("providerEndpoint") or ""
    host_id = new_uuid()
    arn = _host_arn(host_id)
    record = {
        "Name": name,
        "HostArn": arn,
        "ProviderType": provider,
        "ProviderEndpoint": endpoint,
        "Status": "AVAILABLE",
    }
    _hosts[host_id] = record
    return json_response({"HostArn": arn, "Tags": []})


def _get_host(data):
    arn = data.get("HostArn") or data.get("hostArn") or ""
    if "host/" in arn:
        host_id = arn.rsplit("/", 1)[-1]
        host = _hosts.get(host_id)
        if host:
            return json_response({
                "Name": host["Name"],
                "Status": host["Status"],
                "ProviderType": host["ProviderType"],
                "ProviderEndpoint": host["ProviderEndpoint"],
                "VpcConfiguration": host.get("VpcConfiguration", {}),
            })
    return error_response_json("ResourceNotFoundException", f"Host not found: {arn}", 400)


def _delete_host(data):
    arn = data.get("HostArn") or data.get("hostArn") or ""
    if "host/" in arn:
        host_id = arn.rsplit("/", 1)[-1]
        _hosts.pop(host_id, None)
    return json_response({})


def _list_hosts(data):
    return json_response({"Hosts": list(_hosts.values()), "NextToken": None})


def _update_host(data):
    arn = data.get("HostArn") or data.get("hostArn") or ""
    if "host/" in arn:
        host_id = arn.rsplit("/", 1)[-1]
        host = _hosts.get(host_id)
        if host:
            if "ProviderEndpoint" in data:
                host["ProviderEndpoint"] = data["ProviderEndpoint"]
            if "VpcConfiguration" in data:
                host["VpcConfiguration"] = data["VpcConfiguration"]
    return json_response({})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("ResourceArn") or data.get("resourceArn") or ""
    current = _tags.get(arn, {})
    for t in data.get("Tags") or data.get("tags") or []:
        current[t["Key"]] = t["Value"]
    _tags[arn] = current
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceArn") or data.get("resourceArn") or ""
    current = _tags.get(arn, {})
    for k in data.get("TagKeys") or data.get("tagKeys") or []:
        current.pop(k, None)
    _tags[arn] = current
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceArn") or data.get("resourceArn") or ""
    return json_response({
        "Tags": [{"Key": k, "Value": v} for k, v in _tags.get(arn, {}).items()],
    })
