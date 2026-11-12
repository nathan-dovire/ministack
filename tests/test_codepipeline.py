"""
CodePipeline service tests — covers CRUD, executions, state, action types,
webhooks, tags, and edge cases.

The Tier 2 runtime (codepipeline_runtime) is exercised separately in
test_codepipeline_runtime.py.
"""

import time
import pytest
from botocore.exceptions import ClientError


PIPELINE_NAME = "test-pipeline-cp"


def _minimal_pipeline(name=PIPELINE_NAME):
    """Smallest pipeline shape boto3 accepts: a Source -> Build flow."""
    return {
        "name": name,
        "roleArn": "arn:aws:iam::000000000000:role/codepipeline-role",
        "artifactStore": {"type": "S3", "location": "test-artifact-bucket"},
        "stages": [
            {
                "name": "Source",
                "actions": [
                    {
                        "name": "SourceAction",
                        "actionTypeId": {
                            "category": "Source",
                            "owner": "AWS",
                            "provider": "S3",
                            "version": "1",
                        },
                        "configuration": {"S3Bucket": "src", "S3ObjectKey": "v1.zip"},
                        "outputArtifacts": [{"name": "SourceOutput"}],
                    },
                ],
            },
            {
                "name": "Build",
                "actions": [
                    {
                        "name": "BuildAction",
                        "actionTypeId": {
                            "category": "Build",
                            "owner": "AWS",
                            "provider": "CodeBuild",
                            "version": "1",
                        },
                        "configuration": {"ProjectName": "test-cb-project"},
                        "inputArtifacts": [{"name": "SourceOutput"}],
                        "outputArtifacts": [{"name": "BuildOutput"}],
                    },
                ],
            },
        ],
    }


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def test_codepipeline_create(codepipeline):
    out = codepipeline.create_pipeline(pipeline=_minimal_pipeline())
    p = out["pipeline"]
    assert p["name"] == PIPELINE_NAME
    assert p["version"] == 1
    assert p["roleArn"].startswith("arn:aws:iam::")
    assert len(p["stages"]) == 2


def test_codepipeline_create_duplicate_raises(codepipeline):
    with pytest.raises(ClientError) as exc:
        codepipeline.create_pipeline(pipeline=_minimal_pipeline())
    assert "PipelineNameInUseException" in str(exc.value)


def test_codepipeline_create_requires_name(codepipeline):
    from botocore.exceptions import ParamValidationError
    bad = _minimal_pipeline(name="")
    # boto3 enforces min length on the wire, server-side also rejects.
    with pytest.raises((ClientError, ParamValidationError)) as exc:
        codepipeline.create_pipeline(pipeline=bad)
    msg = str(exc.value)
    assert ("ValidationException" in msg
            or "Pipeline name" in msg
            or "min length" in msg.lower())


def test_codepipeline_get(codepipeline):
    out = codepipeline.get_pipeline(name=PIPELINE_NAME)
    p = out["pipeline"]
    md = out["metadata"]
    assert p["name"] == PIPELINE_NAME
    assert md["pipelineArn"].endswith(f":{PIPELINE_NAME}")
    # awsjson1.0 timestamps: boto3 parses as datetime.
    assert md["created"] is not None
    assert md["updated"] is not None


def test_codepipeline_get_nonexistent_raises(codepipeline):
    with pytest.raises(ClientError) as exc:
        codepipeline.get_pipeline(name="no-such-pipeline")
    assert "PipelineNotFoundException" in str(exc.value)


def test_codepipeline_update_bumps_version(codepipeline):
    p = _minimal_pipeline()
    # Mutate the role arn so the update is meaningful.
    p["roleArn"] = "arn:aws:iam::000000000000:role/updated-role"
    out = codepipeline.update_pipeline(pipeline=p)
    assert out["pipeline"]["version"] == 2
    assert out["pipeline"]["roleArn"].endswith("/updated-role")


def test_codepipeline_update_nonexistent_raises(codepipeline):
    with pytest.raises(ClientError) as exc:
        codepipeline.update_pipeline(pipeline=_minimal_pipeline(name="ghost"))
    assert "PipelineNotFoundException" in str(exc.value)


def test_codepipeline_list_includes_created(codepipeline):
    out = codepipeline.list_pipelines()
    names = [p["name"] for p in out["pipelines"]]
    assert PIPELINE_NAME in names
    # Every listed pipeline carries a created/updated timestamp parseable by boto.
    for p in out["pipelines"]:
        if p["name"] == PIPELINE_NAME:
            assert p["created"] is not None
            assert p["updated"] is not None
            assert p["pipelineType"] in ("V1", "V2")


# ---------------------------------------------------------------------------
# Executions
# ---------------------------------------------------------------------------

def test_codepipeline_start_execution_returns_uuid(codepipeline):
    out = codepipeline.start_pipeline_execution(name=PIPELINE_NAME)
    assert "pipelineExecutionId" in out
    assert len(out["pipelineExecutionId"]) >= 32
    # Tier 1 stub or Tier 2 — either way the id is returned.


def test_codepipeline_start_execution_nonexistent_raises(codepipeline):
    with pytest.raises(ClientError) as exc:
        codepipeline.start_pipeline_execution(name="no-such-pipeline")
    assert "PipelineNotFoundException" in str(exc.value)


def test_codepipeline_get_execution(codepipeline):
    started = codepipeline.start_pipeline_execution(name=PIPELINE_NAME)
    eid = started["pipelineExecutionId"]
    # Tier 2 runtime is async; wait briefly for the executor to populate state.
    for _ in range(20):
        out = codepipeline.get_pipeline_execution(
            pipelineName=PIPELINE_NAME, pipelineExecutionId=eid
        )
        if out["pipelineExecution"]["status"] in ("Succeeded", "Failed", "Stopped"):
            break
        time.sleep(0.2)
    pe = out["pipelineExecution"]
    assert pe["pipelineExecutionId"] == eid
    assert pe["pipelineName"] == PIPELINE_NAME
    assert pe["status"] in ("Succeeded", "Failed", "InProgress", "Stopped")


def test_codepipeline_get_execution_unknown_id(codepipeline):
    with pytest.raises(ClientError) as exc:
        codepipeline.get_pipeline_execution(
            pipelineName=PIPELINE_NAME,
            pipelineExecutionId="00000000-0000-0000-0000-000000000000",
        )
    assert "PipelineExecutionNotFoundException" in str(exc.value)


def test_codepipeline_list_executions(codepipeline):
    out = codepipeline.list_pipeline_executions(pipelineName=PIPELINE_NAME)
    # We've started at least 2 executions above.
    assert len(out["pipelineExecutionSummaries"]) >= 2
    # Summaries should be DESCENDING by startTime.
    times = [s.get("startTime") for s in out["pipelineExecutionSummaries"] if s.get("startTime")]
    assert times == sorted(times, reverse=True)


def test_codepipeline_stop_execution(codepipeline):
    started = codepipeline.start_pipeline_execution(name=PIPELINE_NAME)
    eid = started["pipelineExecutionId"]
    out = codepipeline.stop_pipeline_execution(
        pipelineName=PIPELINE_NAME, pipelineExecutionId=eid, abandon=True
    )
    assert out["pipelineExecutionId"] == eid


def test_codepipeline_stop_unknown_execution(codepipeline):
    with pytest.raises(ClientError) as exc:
        codepipeline.stop_pipeline_execution(
            pipelineName=PIPELINE_NAME,
            pipelineExecutionId="00000000-0000-0000-0000-000000000000",
            abandon=True,
        )
    assert "PipelineExecutionNotFoundException" in str(exc.value)


# ---------------------------------------------------------------------------
# Pipeline state
# ---------------------------------------------------------------------------

def test_codepipeline_get_state(codepipeline):
    out = codepipeline.get_pipeline_state(name=PIPELINE_NAME)
    assert out["pipelineName"] == PIPELINE_NAME
    # Stage states present and named correctly.
    stage_names = {s["stageName"] for s in out["stageStates"]}
    assert stage_names == {"Source", "Build"}
    # Every action_state has actionName + latestExecution
    for stage in out["stageStates"]:
        for a in stage["actionStates"]:
            assert a["actionName"]
            assert "latestExecution" in a


def test_codepipeline_get_state_nonexistent(codepipeline):
    with pytest.raises(ClientError) as exc:
        codepipeline.get_pipeline_state(name="no-such")
    assert "PipelineNotFoundException" in str(exc.value)


# ---------------------------------------------------------------------------
# Action executions
# ---------------------------------------------------------------------------

def test_codepipeline_list_action_executions(codepipeline):
    out = codepipeline.list_action_executions(pipelineName=PIPELINE_NAME)
    # Should reflect actions across all executions
    details = out["actionExecutionDetails"]
    assert isinstance(details, list)
    # Each entry has the expected shape
    for d in details:
        assert "pipelineExecutionId" in d
        assert "stageName" in d
        assert "actionName" in d
        assert d["status"] in ("Succeeded", "Failed", "InProgress", "Stopped")


# ---------------------------------------------------------------------------
# Action types catalogue
# ---------------------------------------------------------------------------

def test_codepipeline_list_action_types(codepipeline):
    out = codepipeline.list_action_types()
    by_provider = {(t["id"]["category"], t["id"]["provider"]) for t in out["actionTypes"]}
    # CDK Pipelines actually uses these:
    assert ("Source", "CodeStarSourceConnection") in by_provider
    assert ("Source", "S3") in by_provider
    assert ("Build", "CodeBuild") in by_provider
    assert ("Deploy", "CloudFormation") in by_provider
    assert ("Approval", "Manual") in by_provider


# ---------------------------------------------------------------------------
# Webhooks (stub coverage)
# ---------------------------------------------------------------------------

WEBHOOK_NAME = "test-webhook"


def test_codepipeline_put_webhook(codepipeline):
    out = codepipeline.put_webhook(webhook={
        "name": WEBHOOK_NAME,
        "targetPipeline": PIPELINE_NAME,
        "targetAction": "SourceAction",
        "filters": [{"jsonPath": "$.ref", "matchEquals": "refs/heads/main"}],
        "authentication": "UNAUTHENTICATED",
        "authenticationConfiguration": {},
    })
    wh = out["webhook"]
    assert wh["arn"].startswith("arn:aws:codepipeline:")
    assert wh["url"].endswith(f"/{WEBHOOK_NAME}")


def test_codepipeline_list_webhooks(codepipeline):
    out = codepipeline.list_webhooks()
    assert any(wh["definition"]["name"] == WEBHOOK_NAME for wh in out["webhooks"])


def test_codepipeline_delete_webhook(codepipeline):
    codepipeline.delete_webhook(name=WEBHOOK_NAME)
    out = codepipeline.list_webhooks()
    assert not any(wh["definition"]["name"] == WEBHOOK_NAME for wh in out["webhooks"])


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def test_codepipeline_tag_and_list(codepipeline):
    arn = codepipeline.get_pipeline(name=PIPELINE_NAME)["metadata"]["pipelineArn"]
    codepipeline.tag_resource(
        resourceArn=arn,
        tags=[{"key": "env", "value": "test"}, {"key": "owner", "value": "ministack"}],
    )
    out = codepipeline.list_tags_for_resource(resourceArn=arn)
    tags = {t["key"]: t["value"] for t in out["tags"]}
    assert tags["env"] == "test"
    assert tags["owner"] == "ministack"


def test_codepipeline_untag(codepipeline):
    arn = codepipeline.get_pipeline(name=PIPELINE_NAME)["metadata"]["pipelineArn"]
    codepipeline.untag_resource(resourceArn=arn, tagKeys=["env"])
    out = codepipeline.list_tags_for_resource(resourceArn=arn)
    keys = {t["key"] for t in out["tags"]}
    assert "env" not in keys
    assert "owner" in keys


# ---------------------------------------------------------------------------
# Worker API stubs (do-nothing but should respond)
# ---------------------------------------------------------------------------

def test_codepipeline_poll_for_jobs_empty(codepipeline):
    out = codepipeline.poll_for_jobs(actionTypeId={
        "category": "Build", "owner": "Custom",
        "provider": "Custom", "version": "1",
    })
    assert out["jobs"] == []


# ---------------------------------------------------------------------------
# Manual approval gate — pause + resume (Tier 2 runtime)
# ---------------------------------------------------------------------------

def _approval_pipeline(name):
    """A pipeline with a manual-approval gate, then a no-op stage after it."""
    return {
        "name": name,
        "roleArn": "arn:aws:iam::000000000000:role/codepipeline-role",
        "artifactStore": {"type": "S3", "location": "test-artifact-bucket"},
        "stages": [
            {"name": "Gate", "actions": [{
                "name": "Approve",
                "actionTypeId": {"category": "Approval", "owner": "AWS",
                                 "provider": "Manual", "version": "1"},
                "configuration": {},
            }]},
            {"name": "Post", "actions": [{
                "name": "NoOp",
                "actionTypeId": {"category": "Compute", "owner": "AWS",
                                 "provider": "NoOp", "version": "1"},
                "configuration": {},
            }]},
        ],
    }


def _poll_execution(cp, name, eid, predicate, timeout=20):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = cp.get_pipeline_execution(
            pipelineName=name, pipelineExecutionId=eid)["pipelineExecution"]
        if predicate(last):
            return last
        time.sleep(0.5)
    return last


def test_codepipeline_manual_approval_pauses_then_resumes(codepipeline):
    """By default a manual-approval action pauses the run (real-AWS behaviour),
    leaving it InProgress until PutApprovalResult; Approved resumes it."""
    name = "approval-resume-cp"
    codepipeline.create_pipeline(pipeline=_approval_pipeline(name))
    eid = codepipeline.start_pipeline_execution(name=name)["pipelineExecutionId"]

    paused = _poll_execution(codepipeline, name, eid,
                             lambda e: "Waiting for approval" in (e.get("statusSummary") or ""))
    assert paused["status"] == "InProgress"
    assert "Gate/Approve" in paused["statusSummary"]

    codepipeline.put_approval_result(
        pipelineName=name, stageName="Gate", actionName="Approve",
        result={"summary": "ok", "status": "Approved"}, token="approve")

    done = _poll_execution(codepipeline, name, eid, lambda e: e["status"] in ("Succeeded", "Failed"))
    assert done["status"] == "Succeeded"


def test_codepipeline_manual_approval_reject_fails(codepipeline):
    name = "approval-reject-cp"
    codepipeline.create_pipeline(pipeline=_approval_pipeline(name))
    eid = codepipeline.start_pipeline_execution(name=name)["pipelineExecutionId"]
    _poll_execution(codepipeline, name, eid,
                    lambda e: "Waiting for approval" in (e.get("statusSummary") or ""))

    codepipeline.put_approval_result(
        pipelineName=name, stageName="Gate", actionName="Approve",
        result={"summary": "nope", "status": "Rejected"}, token="approve")

    done = _poll_execution(codepipeline, name, eid, lambda e: e["status"] in ("Succeeded", "Failed"))
    assert done["status"] == "Failed"
    assert "reject" in done["statusSummary"].lower()


def test_codepipeline_put_approval_result_no_pending(codepipeline):
    """PutApprovalResult with no matching paused gate errors clearly."""
    name = "approval-none-cp"
    codepipeline.create_pipeline(pipeline=_approval_pipeline(name))
    with pytest.raises(ClientError) as exc:
        codepipeline.put_approval_result(
            pipelineName=name, stageName="Gate", actionName="Approve",
            result={"summary": "x", "status": "Approved"}, token="approve")
    assert "ApprovalAlreadyCompletedException" in str(exc.value)


# ---------------------------------------------------------------------------
# Delete (final cleanup test)
# ---------------------------------------------------------------------------

def test_codepipeline_delete(codepipeline):
    # Deleting a non-existent pipeline is a no-op in real AWS (and ours).
    codepipeline.delete_pipeline(name="never-existed-pipeline")
    # Deleting the real one removes it and its executions.
    codepipeline.delete_pipeline(name=PIPELINE_NAME)
    names = [p["name"] for p in codepipeline.list_pipelines()["pipelines"]]
    assert PIPELINE_NAME not in names
    # Subsequent get raises NotFound.
    with pytest.raises(ClientError) as exc:
        codepipeline.get_pipeline(name=PIPELINE_NAME)
    assert "PipelineNotFoundException" in str(exc.value)
