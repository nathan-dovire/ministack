"""
CloudFormation provisioner tests for the CodePipeline / CodeStar Connections /
CodeBuild::ReportGroup resource types added by the Tier 2 work.

Validates idempotent re-application (for SelfMutate / re-deploy scenarios),
deletion handlers, and that the resources actually land in the underlying
service registries.
"""

import json
import time
import pytest
from botocore.exceptions import ClientError


def _wait_complete(cfn, stack_name, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = cfn.describe_stacks(StackName=stack_name)
        s = r["Stacks"][0]["StackStatus"]
        if s.endswith("_COMPLETE"):
            return s
        if "FAILED" in s:
            return s
        time.sleep(1)
    raise AssertionError(f"{stack_name} did not finish: last status {s}")


# ---------------------------------------------------------------------------
# AWS::CodePipeline::Pipeline
# ---------------------------------------------------------------------------

_PIPELINE_TEMPLATE = {
    "Resources": {
        "ArtifactBucket": {
            "Type": "AWS::S3::Bucket",
        },
        "Pipeline": {
            "Type": "AWS::CodePipeline::Pipeline",
            "Properties": {
                "Name": "cfn-test-pipeline",
                "RoleArn": "arn:aws:iam::000000000000:role/cfn-pl-role",
                "ArtifactStore": {
                    "Type": "S3",
                    "Location": {"Ref": "ArtifactBucket"},
                },
                "Stages": [
                    {
                        "Name": "Source",
                        "Actions": [
                            {
                                "Name": "S3Source",
                                "ActionTypeId": {
                                    "Category": "Source",
                                    "Owner": "AWS",
                                    "Provider": "S3",
                                    "Version": "1",
                                },
                                "Configuration": {
                                    "S3Bucket": "any",
                                    "S3ObjectKey": "src.zip",
                                },
                                "OutputArtifacts": [{"Name": "src"}],
                            }
                        ],
                    },
                    {
                        "Name": "Deploy",
                        "Actions": [
                            {
                                "Name": "DeployCFN",
                                "ActionTypeId": {
                                    "Category": "Deploy",
                                    "Owner": "AWS",
                                    "Provider": "CloudFormation",
                                    "Version": "1",
                                },
                                "Configuration": {
                                    "ActionMode": "CHANGE_SET_REPLACE",
                                    "StackName": "downstream",
                                    "TemplatePath": "src::downstream.json",
                                    "ChangeSetName": "cs",
                                },
                                "InputArtifacts": [{"Name": "src"}],
                            }
                        ],
                    },
                ],
            },
        },
    },
}


def test_cfn_codepipeline_pipeline_create(cfn, codepipeline):
    cloudformation = cfn
    cloudformation.create_stack(
        StackName="cfn-pl-stack",
        TemplateBody=json.dumps(_PIPELINE_TEMPLATE),
        Capabilities=["CAPABILITY_IAM"],
    )
    assert _wait_complete(cloudformation, "cfn-pl-stack") == "CREATE_COMPLETE"

    # Pipeline landed in codepipeline service.
    listed = codepipeline.list_pipelines()
    names = {p["name"] for p in listed["pipelines"]}
    assert "cfn-test-pipeline" in names


def test_cfn_codepipeline_pipeline_update_idempotent(cfn, codepipeline):
    cloudformation = cfn
    """SelfMutate re-applies the same template; CFN provisioner must update in
    place rather than raise PipelineNameInUseException."""
    cloudformation.update_stack(
        StackName="cfn-pl-stack",
        TemplateBody=json.dumps(_PIPELINE_TEMPLATE),
        Capabilities=["CAPABILITY_IAM"],
    )
    # Either UPDATE_COMPLETE or no-op succeeds.
    final = _wait_complete(cloudformation, "cfn-pl-stack")
    assert final.endswith("_COMPLETE")
    # Still exactly one pipeline by that name.
    pipes = [p for p in codepipeline.list_pipelines()["pipelines"]
             if p["name"] == "cfn-test-pipeline"]
    assert len(pipes) == 1
    # Version bumped on re-apply.
    assert pipes[0]["version"] >= 2


def test_cfn_codepipeline_pipeline_delete(cfn, codepipeline):
    cloudformation = cfn
    cloudformation.delete_stack(StackName="cfn-pl-stack")
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            cloudformation.describe_stacks(StackName="cfn-pl-stack")
        except ClientError:
            break
        time.sleep(0.5)
    names = {p["name"] for p in codepipeline.list_pipelines()["pipelines"]}
    assert "cfn-test-pipeline" not in names


# ---------------------------------------------------------------------------
# AWS::CodeBuild::ReportGroup
# ---------------------------------------------------------------------------

def test_cfn_codebuild_report_group(cfn):
    cloudformation = cfn
    template = {
        "Resources": {
            "RG": {
                "Type": "AWS::CodeBuild::ReportGroup",
                "Properties": {
                    "Name": "test-rg",
                    "Type": "TEST",
                    "ExportConfig": {"ExportConfigType": "NO_EXPORT"},
                },
            },
        },
    }
    cloudformation.create_stack(
        StackName="cfn-rg-stack", TemplateBody=json.dumps(template),
    )
    assert _wait_complete(cloudformation, "cfn-rg-stack") == "CREATE_COMPLETE"

    # Clean up so subsequent runs don't accumulate.
    cloudformation.delete_stack(StackName="cfn-rg-stack")


# ---------------------------------------------------------------------------
# AWS::CodeStarConnections::Connection
# ---------------------------------------------------------------------------

def test_cfn_codestar_connection(cfn, codestar_connections):
    cloudformation = cfn
    template = {
        "Resources": {
            "C": {
                "Type": "AWS::CodeStarConnections::Connection",
                "Properties": {
                    "ConnectionName": "cfn-bitbucket-conn",
                    "ProviderType": "Bitbucket",
                },
            },
        },
        "Outputs": {
            "ConnectionArn": {"Value": {"Ref": "C"}},
        },
    }
    cloudformation.create_stack(
        StackName="cfn-csc-stack", TemplateBody=json.dumps(template),
    )
    assert _wait_complete(cloudformation, "cfn-csc-stack") == "CREATE_COMPLETE"

    listed = codestar_connections.list_connections()
    names = {c["ConnectionName"] for c in listed["Connections"]}
    assert "cfn-bitbucket-conn" in names

    cloudformation.delete_stack(StackName="cfn-csc-stack")
