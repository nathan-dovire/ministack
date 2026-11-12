"""
CodeStar Connections service tests.
Covers CRUD, hosts, tagging, and the local-source-path resolution that the
CodePipeline runtime uses.
"""

import os
import pytest
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Connections — CRUD
# ---------------------------------------------------------------------------

def test_csc_create_connection_bitbucket(codestar_connections):
    out = codestar_connections.create_connection(
        ConnectionName="test-bitbucket-conn", ProviderType="Bitbucket"
    )
    arn = out["ConnectionArn"]
    assert arn.startswith("arn:aws:codestar-connections:")
    assert "connection/" in arn


def test_csc_create_connection_github(codestar_connections):
    out = codestar_connections.create_connection(
        ConnectionName="test-github-conn", ProviderType="GitHub"
    )
    assert "connection/" in out["ConnectionArn"]


def test_csc_create_requires_name(codestar_connections):
    from botocore.exceptions import ParamValidationError
    with pytest.raises((ClientError, ParamValidationError)) as exc:
        codestar_connections.create_connection(
            ConnectionName="", ProviderType="Bitbucket"
        )
    msg = str(exc.value)
    assert ("ValidationException" in msg
            or "ConnectionName" in msg
            or "min length" in msg.lower()
            or "required" in msg.lower())


def test_csc_get_connection_by_arn(codestar_connections):
    listed = codestar_connections.list_connections()["Connections"]
    arn = next(c["ConnectionArn"] for c in listed if c["ConnectionName"] == "test-bitbucket-conn")
    out = codestar_connections.get_connection(ConnectionArn=arn)
    c = out["Connection"]
    assert c["ConnectionName"] == "test-bitbucket-conn"
    assert c["ProviderType"] == "Bitbucket"
    # AWS lifecycle: PENDING until OAuth completion. MiniStack short-circuits
    # to AVAILABLE so CDK pipelines that look up the connection ARN succeed.
    assert c["ConnectionStatus"] == "AVAILABLE"
    assert c["OwnerAccountId"]


def test_csc_get_unknown_arn_auto_provisions(codestar_connections):
    """A CDK pipeline whose template baked in a connection ARN should not break
    just because no CreateConnection call ran. We auto-provision an
    AVAILABLE record so GetConnection succeeds — matches the documented behaviour
    of MiniStack's codestar_connections._get_connection fallback."""
    fake_arn = (
        "arn:aws:codestar-connections:us-east-1:000000000000:"
        "connection/deadbeef-cafe-1234-5678-1234567890ab"
    )
    out = codestar_connections.get_connection(ConnectionArn=fake_arn)
    assert out["Connection"]["ConnectionStatus"] == "AVAILABLE"


def test_csc_get_malformed_arn_raises(codestar_connections):
    with pytest.raises(ClientError):
        codestar_connections.get_connection(ConnectionArn="not-an-arn")


def test_csc_list_connections(codestar_connections):
    out = codestar_connections.list_connections()
    names = {c["ConnectionName"] for c in out["Connections"]}
    assert "test-bitbucket-conn" in names
    assert "test-github-conn" in names


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

def test_csc_tag_resource(codestar_connections):
    arn = next(
        c["ConnectionArn"]
        for c in codestar_connections.list_connections()["Connections"]
        if c["ConnectionName"] == "test-bitbucket-conn"
    )
    codestar_connections.tag_resource(
        ResourceArn=arn,
        Tags=[{"Key": "env", "Value": "ministack"}],
    )
    out = codestar_connections.list_tags_for_resource(ResourceArn=arn)
    tags = {t["Key"]: t["Value"] for t in out["Tags"]}
    assert tags["env"] == "ministack"


def test_csc_untag(codestar_connections):
    arn = next(
        c["ConnectionArn"]
        for c in codestar_connections.list_connections()["Connections"]
        if c["ConnectionName"] == "test-bitbucket-conn"
    )
    codestar_connections.untag_resource(ResourceArn=arn, TagKeys=["env"])
    out = codestar_connections.list_tags_for_resource(ResourceArn=arn)
    keys = {t["Key"] for t in out["Tags"]}
    assert "env" not in keys


# ---------------------------------------------------------------------------
# Hosts
# ---------------------------------------------------------------------------

def test_csc_create_and_get_host(codestar_connections):
    out = codestar_connections.create_host(
        Name="my-ghes-host",
        ProviderType="GitHubEnterpriseServer",
        ProviderEndpoint="https://github.example.com",
    )
    arn = out["HostArn"]
    assert "host/" in arn

    info = codestar_connections.get_host(HostArn=arn)
    assert info["Name"] == "my-ghes-host"
    assert info["Status"] == "AVAILABLE"
    assert info["ProviderType"] == "GitHubEnterpriseServer"


def test_csc_get_unknown_host_raises(codestar_connections):
    with pytest.raises(ClientError) as exc:
        codestar_connections.get_host(
            HostArn="arn:aws:codestar-connections:us-east-1:000000000000:host/nope"
        )
    assert "ResourceNotFoundException" in str(exc.value)


def test_csc_list_hosts(codestar_connections):
    out = codestar_connections.list_hosts()
    assert any(h["Name"] == "my-ghes-host" for h in out["Hosts"])


def test_csc_delete_host(codestar_connections):
    arn = next(h.get("HostArn") for h in codestar_connections.list_hosts()["Hosts"]
               if h["Name"] == "my-ghes-host")
    if arn:
        codestar_connections.delete_host(HostArn=arn)


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------

def test_csc_delete_connection(codestar_connections):
    arn = next(
        c["ConnectionArn"]
        for c in codestar_connections.list_connections()["Connections"]
        if c["ConnectionName"] == "test-bitbucket-conn"
    )
    codestar_connections.delete_connection(ConnectionArn=arn)
    out = codestar_connections.list_connections()
    names = {c["ConnectionName"] for c in out["Connections"]}
    assert "test-bitbucket-conn" not in names


def test_csc_delete_idempotent(codestar_connections):
    # Deleting an already-deleted (or never-existed) ARN should be a no-op.
    codestar_connections.delete_connection(
        ConnectionArn="arn:aws:codestar-connections:us-east-1:000000000000:connection/ghost"
    )
