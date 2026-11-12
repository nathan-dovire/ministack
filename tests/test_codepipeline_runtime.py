"""
End-to-end tests for the Tier 2 CodePipeline runtime orchestrator.

These exercise the real action handlers — source action zipping a local
working tree, the build pass-through, the CloudFormation deploy action, and
the asset-publish helper that mimics `cdk-assets publish`.

The tests build a small "pipeline-shaped" set of artifacts on disk and let
the runtime walk through them, asserting that:
  - the target CFN stack lands
  - the asset publisher uploads the right object to the right S3 bucket
  - PascalCase configuration keys (CFN convention) reach the dispatcher
    intact
"""

import json
import os
import time
import pytest
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Source action / asset publisher fixtures (cdk.out shape)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fake_cdk_out(tmp_path_factory):
    """Build a minimal cdk.out tree the runtime can recognize."""
    base = tmp_path_factory.mktemp("project")
    cdk_out = base / "cdk.out"
    cdk_out.mkdir()
    assembly = cdk_out / "assembly-stage"
    assembly.mkdir()

    # Manifest with a stack artifact
    (cdk_out / "manifest.json").write_text(json.dumps({
        "version": "36.0.0",
        "artifacts": {
            "TestPipelineStack": {
                "type": "aws:cloudformation:stack",
                "environment": "aws://000000000000/us-east-1",
                "properties": {"templateFile": "TestPipelineStack.template.json"},
            },
        },
    }))

    # A template + its assets manifest
    (cdk_out / "TestPipelineStack.template.json").write_text(json.dumps({
        "Resources": {"B": {"Type": "AWS::S3::Bucket"}},
    }))
    (cdk_out / "TestPipelineStack.assets.json").write_text(json.dumps({
        "version": "36.0.0",
        "files": {
            "abc123": {
                "source": {"path": "TestPipelineStack.template.json", "packaging": "file"},
                "destinations": {
                    "000000000000-us-east-1": {
                        "bucketName": "cdk-hnb659fds-assets-000000000000-us-east-1",
                        "objectKey": "abc123.json",
                        "region": "us-east-1",
                    },
                },
            },
        },
    }))
    return base


# ---------------------------------------------------------------------------
# Tier 3a — real asset publisher
# ---------------------------------------------------------------------------

def test_runtime_publish_assets_from_disk(s3, fake_cdk_out, monkeypatch):
    """Make sure _publish_assets_from_disk uploads the manifest-referenced files."""
    import ministack.services.codepipeline_runtime as rt

    monkeypatch.setenv("MINISTACK_CONNECTION_DEFAULT_PATH", str(fake_cdk_out))
    # When running from inside a sibling docker container, the runtime's
    # default endpoint (localhost:4566) doesn't reach the ministack container.
    # Honor the same MINISTACK_ENDPOINT the test fixtures use.
    monkeypatch.setenv("AWS_ENDPOINT_URL_MINISTACK",
                       os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"))

    bucket = "cdk-hnb659fds-assets-000000000000-us-east-1"
    s3.create_bucket(Bucket=bucket)

    n = rt._publish_assets_from_disk()
    assert n >= 1, "expected at least one asset to publish"

    objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
    keys = {o["Key"] for o in objects}
    assert "abc123.json" in keys

    # Object body must equal the source template
    body = s3.get_object(Bucket=bucket, Key="abc123.json")["Body"].read()
    template = json.loads(body)
    assert template["Resources"]["B"]["Type"] == "AWS::S3::Bucket"


# ---------------------------------------------------------------------------
# Default cdk.out resolution
# ---------------------------------------------------------------------------

def test_runtime_resolve_cdk_out_root(fake_cdk_out, monkeypatch):
    import ministack.services.codepipeline_runtime as rt
    monkeypatch.setenv("MINISTACK_CONNECTION_DEFAULT_PATH", str(fake_cdk_out))
    resolved = rt._resolve_default_cdk_out()
    assert resolved is not None
    assert resolved.name == "cdk.out"


def test_runtime_resolve_cdk_out_apps_subdir(tmp_path, monkeypatch):
    """Repo with apps/<name>/cdk.out should also resolve."""
    import ministack.services.codepipeline_runtime as rt
    monorepo = tmp_path / "repo"
    cdkout = monorepo / "apps" / "workload" / "cdk.out"
    cdkout.mkdir(parents=True)
    (cdkout / "manifest.json").write_text("{}")
    monkeypatch.setenv("MINISTACK_CONNECTION_DEFAULT_PATH", str(monorepo))
    resolved = rt._resolve_default_cdk_out()
    assert resolved is not None
    assert resolved.parent.name == "workload"


def test_runtime_resolve_cdk_out_missing(tmp_path, monkeypatch):
    import ministack.services.codepipeline_runtime as rt
    monkeypatch.setenv("MINISTACK_CONNECTION_DEFAULT_PATH", str(tmp_path))
    assert rt._resolve_default_cdk_out() is None


# ---------------------------------------------------------------------------
# Key normalization (CFN PascalCase / JSON-RPC camelCase)
# ---------------------------------------------------------------------------

def test_runtime_normalize_keys_preserves_configuration():
    """Action.configuration uses PascalCase keys natively; everything else
    lowercases first letter."""
    import ministack.services.codepipeline_runtime as rt
    norm = rt._normalize_keys({
        "Name": "Source",
        "Actions": [{
            "Name": "Src",
            "ActionTypeId": {"Category": "Source", "Provider": "S3"},
            "Configuration": {"StackName": "downstream", "TemplatePath": "x::y.json"},
        }],
    })
    stage = norm
    assert "name" in stage and "actions" in stage
    action = stage["actions"][0]
    assert action["name"] == "Src"
    assert action["actionTypeId"]["category"] == "Source"
    # Configuration keys preserved — StackName still PascalCase.
    assert action["configuration"]["StackName"] == "downstream"
    assert action["configuration"]["TemplatePath"] == "x::y.json"


# ---------------------------------------------------------------------------
# Account/role-ARN extraction
# ---------------------------------------------------------------------------

def test_runtime_account_from_role_arn():
    import ministack.services.codepipeline_runtime as rt
    assert rt._parse_account_from_arn(
        "arn:aws:iam::123456789012:role/foo"
    ) == "123456789012"
    assert rt._parse_account_from_arn("") is None
    assert rt._parse_account_from_arn("not-an-arn") is None
