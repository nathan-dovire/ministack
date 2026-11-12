"""
CodePipeline execution runtime (Tier 2 orchestrator).

Iterates a pipeline definition's stages -> actions and dispatches each action
to a category/provider handler. Designed for "local source -> CFN deploy"
pipelines (the CDK Pipelines shape): Source from a CodeStar Connection mapped
to a local working tree, no-op CodeBuild stages (the artifact is treated as
already-synthed CDK output), CFN deploy actions hitting MiniStack itself.

Registered with codepipeline.set_executor() at module import time, so the
runtime is on by default once MiniStack restarts. Disable by setting
MINISTACK_CODEPIPELINE_RUNTIME=0 (then executions auto-succeed with no work).
"""

import asyncio
import io
import json
import logging
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
import zipfile
from pathlib import Path

import ministack.services.codepipeline as _codepipeline
import ministack.services.codestar_connections as _codestar_conn

logger = logging.getLogger("codepipeline-runtime")

_ARTIFACT_BUCKET_DEFAULT = os.environ.get("MINISTACK_PIPELINE_ARTIFACT_BUCKET", "ministack-codepipeline-artifacts")

# Executions paused at a manual-approval action, awaiting PutApprovalResult.
# execution_id -> {pipeline_def, stage_idx, action_idx, stage_name,
#                  action_name, artifacts}. In-memory only (artifacts are
# bytes); a MiniStack restart drops pending approvals — re-run the pipeline.
_PAUSED: dict = {}


# ---------------------------------------------------------------------------
# Action dispatch
# ---------------------------------------------------------------------------

_PRESERVE_CASE_KEYS = {"configuration", "Configuration"}


def _normalize_keys(value, parent_key=None):
    """Recursively lowercase first char of dict keys.

    Action.configuration is service-specific and uses PascalCase keys
    (StackName, TemplatePath, ActionMode, etc.) in both CFN and the JSON RPC
    CodePipeline API, so it's preserved verbatim.
    """
    if parent_key in _PRESERVE_CASE_KEYS:
        return value
    if isinstance(value, dict):
        return {
            (k[:1].lower() + k[1:] if k else k): _normalize_keys(v, parent_key=k)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_normalize_keys(v, parent_key=parent_key) for v in value]
    return value


async def _run_pipeline(pipeline_def, execution_record, *,
                        start_stage=0, start_action=0, artifacts=None):
    """Top-level orchestrator: walk stages -> actions sequentially.

    Pauses at a manual-approval action (real-AWS behaviour): the execution is
    left InProgress, awaiting PutApprovalResult, and stages after the gate do
    NOT run. Set MINISTACK_CODEPIPELINE_AUTO_APPROVE=1 to auto-approve instead
    (the old behaviour). `start_stage`/`start_action`/`artifacts` let
    resume_paused() continue a run after approval.
    """
    pipeline_def = _normalize_keys(pipeline_def)
    name = pipeline_def["name"]
    execution_id = execution_record["pipelineExecutionId"]
    if (start_stage, start_action) == (0, 0):
        logger.info("Pipeline %s execution %s starting", name, execution_id)
    else:
        logger.info("Pipeline %s execution %s resuming at stage %d / action %d",
                    name, execution_id, start_stage, start_action)

    # Artifact storage: output artifact name → bytes (in-memory for Tier 2).
    if artifacts is None:
        artifacts = {}

    auto_approve = os.environ.get("MINISTACK_CODEPIPELINE_AUTO_APPROVE", "0") == "1"
    # Even with global auto-approve, MINISTACK_PROD_APPROVAL_MANUAL forces a real
    # pause on Production-stage approval gates (real-AWS prod-promotion semantics).
    prod_manual = os.environ.get("MINISTACK_PROD_APPROVAL_MANUAL", "0") == "1"
    stage_states = execution_record["stageStates"]
    stages = pipeline_def.get("stages", [])

    try:
        for stage_idx in range(start_stage, len(stages)):
            stage = stages[stage_idx]
            stage_state = stage_states[stage_idx]
            stage_name = stage["name"]
            logger.info("[%s] Stage: %s", execution_id, stage_name)
            stage_state["latestExecution"] = {
                "pipelineExecutionId": execution_id, "status": "InProgress",
            }

            actions = stage.get("actions", [])
            first_action = start_action if stage_idx == start_stage else 0
            for action_idx in range(first_action, len(actions)):
                action = actions[action_idx]
                action_state = stage_state["actionStates"][action_idx]
                category = action.get("actionTypeId", {}).get("category", "")
                provider = action.get("actionTypeId", {}).get("provider", "")
                action_name = action.get("name", "")

                # Manual approval: pause and await PutApprovalResult. Auto-approve
                # flows straight through, except a Production-stage gate when
                # MINISTACK_PROD_APPROVAL_MANUAL=1. Nothing after the gate runs
                # until approved.
                prod_gate = prod_manual and re.search(r"prod", stage_name, re.IGNORECASE) is not None
                if category == "Approval" and provider == "Manual" and (not auto_approve or prod_gate):
                    logger.info("[%s] Manual approval '%s' — pausing (awaiting approval)",
                                execution_id, action_name)
                    action_state["latestExecution"] = {
                        "status": "InProgress",
                        "summary": "Waiting for approval",
                        "lastStatusChange": _codepipeline._now_iso(),
                    }
                    stage_state["latestExecution"]["status"] = "InProgress"
                    execution_record["status"] = "InProgress"
                    execution_record["statusSummary"] = (
                        f"Waiting for approval: {stage_name}/{action_name}"
                    )
                    execution_record["lastUpdateTime"] = _codepipeline._now_iso()
                    _PAUSED[execution_id] = {
                        "pipeline_def": pipeline_def,
                        "stage_idx": stage_idx,
                        "action_idx": action_idx,
                        "stage_name": stage_name,
                        "action_name": action_name,
                        "artifacts": artifacts,
                    }
                    return  # paused — resume_paused() continues from here

                logger.info("[%s]   Action: %s (%s/%s)", execution_id,
                            action_name, category, provider)
                action_state["latestExecution"] = {
                    "status": "InProgress",
                    "summary": "",
                    "lastStatusChange": _codepipeline._now_iso(),
                }

                try:
                    await _dispatch_action(category, provider, action,
                                           pipeline_def, artifacts, execution_record)
                    action_state["latestExecution"] = {
                        "status": "Succeeded",
                        "summary": f"{category}/{provider} Succeeded",
                        "lastStatusChange": _codepipeline._now_iso(),
                    }
                except Exception as e:
                    logger.exception("[%s] Action %s failed", execution_id, action_name)
                    action_state["latestExecution"] = {
                        "status": "Failed",
                        "summary": f"{type(e).__name__}: {e}",
                        "lastStatusChange": _codepipeline._now_iso(),
                    }
                    if not _continue_on_error():
                        stage_state["latestExecution"]["status"] = "Failed"
                        execution_record["status"] = "Failed"
                        execution_record["statusSummary"] = f"Stage {stage_name} / {action_name}: {e}"
                        execution_record["lastUpdateTime"] = _codepipeline._now_iso()
                        return
                    # Continue-on-error mode (opt-in): keep going so we see
                    # the full pipeline shape exercised. The execution ends
                    # in "Succeeded" but individual actionStates record Failed.

            stage_state["latestExecution"]["status"] = "Succeeded"

        execution_record["status"] = "Succeeded"
        execution_record["statusSummary"] = "Succeeded"
        execution_record["lastUpdateTime"] = _codepipeline._now_iso()
        logger.info("Pipeline %s execution %s SUCCEEDED", name, execution_id)
    except Exception as e:
        logger.exception("Pipeline %s execution %s crashed", name, execution_id)
        execution_record["status"] = "Failed"
        execution_record["statusSummary"] = f"orchestrator crashed: {e}"
        execution_record["lastUpdateTime"] = _codepipeline._now_iso()


def find_paused(pipeline_name, stage_name="", action_name=""):
    """execution_id of a run paused at the given pipeline/stage/action approval
    gate (stage/action optional), or None."""
    for eid, info in _PAUSED.items():
        if info["pipeline_def"].get("name") != pipeline_name:
            continue
        if stage_name and info["stage_name"] != stage_name:
            continue
        if action_name and info["action_name"] != action_name:
            continue
        return eid
    return None


def resume_paused(execution_id, approved, summary=""):
    """Resolve a paused approval. Approved → resume the run after the gate;
    Rejected → fail the stage + execution. Returns True if a paused run was
    found and acted on."""
    info = _PAUSED.pop(execution_id, None)
    if not info:
        return False
    execution = _codepipeline._executions.get(execution_id)
    if not execution:
        return False

    stage_idx, action_idx = info["stage_idx"], info["action_idx"]
    stage_state = execution["stageStates"][stage_idx]
    action_state = stage_state["actionStates"][action_idx]
    now = _codepipeline._now_iso()

    if not approved:
        action_state["latestExecution"] = {
            "status": "Failed", "summary": summary or "Rejected", "lastStatusChange": now,
        }
        stage_state["latestExecution"]["status"] = "Failed"
        execution["status"] = "Failed"
        execution["statusSummary"] = f"Approval rejected: {info['stage_name']}/{info['action_name']}"
        execution["lastUpdateTime"] = now
        logger.info("Pipeline %s execution %s REJECTED at %s/%s",
                    info["pipeline_def"].get("name"), execution_id,
                    info["stage_name"], info["action_name"])
        return True

    # Approved — mark the gate Succeeded and resume after it.
    action_state["latestExecution"] = {
        "status": "Succeeded", "summary": summary or "Approved", "lastStatusChange": now,
    }
    execution["statusSummary"] = "Resuming after approval"
    execution["lastUpdateTime"] = now

    async def _resume_runner():
        try:
            await _run_pipeline(
                info["pipeline_def"], execution,
                start_stage=stage_idx, start_action=action_idx + 1,
                artifacts=info["artifacts"],
            )
        except Exception:
            logger.exception("Resume failed for execution %s", execution_id)
            execution["status"] = "Failed"
            execution["statusSummary"] = "Resume raised"
            execution["lastUpdateTime"] = _codepipeline._now_iso()

    asyncio.create_task(_resume_runner())
    return True


async def _dispatch_action(category, provider, action, pipeline_def, artifacts, execution):
    if category == "Source" and provider == "CodeStarSourceConnection":
        return await _action_source_codestar(action, artifacts, pipeline_def)
    if category == "Source" and provider == "S3":
        return await _action_source_s3(action, artifacts)
    if category == "Build" and provider == "CodeBuild":
        return await _action_build_codebuild(action, artifacts, pipeline_def)
    if category == "Test" and provider == "CodeBuild":
        return await _action_build_codebuild(action, artifacts, pipeline_def)
    if category == "Deploy" and provider == "CloudFormation":
        return await _action_deploy_cfn(action, artifacts, pipeline_def)
    if category == "Approval" and provider == "Manual":
        # Auto-approve in local emulation.
        logger.info("Manual approval action: auto-approving")
        return None
    if category == "Invoke" and provider == "Lambda":
        return await _action_invoke_lambda(action, artifacts)

    logger.warning("Unsupported action %s/%s — treating as no-op", category, provider)


# ---------------------------------------------------------------------------
# Source action — CodeStar Connection -> local repo zip -> output artifact
# ---------------------------------------------------------------------------

async def _action_source_codestar(action, artifacts, pipeline_def=None):
    config = action.get("configuration", {})
    pipeline_name = (pipeline_def or {}).get("name") or ""
    connection_arn = config.get("ConnectionArn") or config.get("connectionArn") or ""
    repo = config.get("FullRepositoryId") or config.get("fullRepositoryId") or ""
    branch = config.get("BranchName") or config.get("branchName") or "main"

    connection = _codestar_conn.lookup_connection_by_arn(connection_arn)
    local_path = None
    if connection:
        local_path = connection.get("LocalSourcePath")
    if not local_path:
        # Fall back to env var default, or auto-resolve.
        local_path = os.environ.get("MINISTACK_CONNECTION_DEFAULT_PATH")
        if not local_path:
            raise RuntimeError(
                f"No local path for connection {connection_arn} (repo {repo}). "
                f"Set MINISTACK_CONNECTION_<NAME>=<path> or MINISTACK_CONNECTION_DEFAULT_PATH."
            )

    src = Path(local_path)
    if not src.exists():
        raise RuntimeError(f"Source path does not exist: {src}")

    required = _pipeline_required_assemblies(pipeline_def)
    zip_bytes = await asyncio.to_thread(_zip_directory, src, pipeline_name, required)
    for art in action.get("outputArtifacts", []):
        artifacts[art["name"]] = zip_bytes
        logger.info("Source action emitted artifact %s (%d bytes from %s)",
                    art["name"], len(zip_bytes), src)


def _pipeline_cdk_out_subdir(pipeline_name: str):
    """Resolve the cdk.out subdir for a pipeline from MINISTACK_PIPELINE_CDK_OUT.

    Format: comma-separated `<pipeline>:<stack-or-*>:<subdir>` entries. This
    monorepo has multiple CDK apps, each with its own cdk.out; a pipeline must
    source from the one its stacks were synthed into (e.g. pipeline `app-a`
    synths to `apps/a/cdk.out`, not `apps/b/cdk.out`). The
    stack field is parsed for forward-compatibility but not matched on: one
    pipeline maps to one app's cloud assembly. Returns the subdir or None.
    """
    if not pipeline_name:
        return None
    raw = os.environ.get("MINISTACK_PIPELINE_CDK_OUT", "")
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) < 3:
            continue
        pl, _stack, subdir = parts[0].strip(), parts[1].strip(), parts[2].strip()
        if pl == pipeline_name and subdir:
            return subdir
    return None


def _pipeline_required_assemblies(pipeline_def):
    """Top-level `assembly-*` dirs this pipeline's deploy actions read from.

    Each Deploy/CloudFormation action carries a TemplatePath like
    `Synth_Output::assembly-PipelineStack-Staging/Foo.template.json`. The first
    path segment names the cloud-assembly dir the template lives in, which is a
    direct child of the app's cdk.out. Collecting these lets the source action
    pick the cdk.out that actually contains them, instead of globbing blindly.
    Returns a set of assembly dir names (empty if none are referenced).
    """
    required = set()
    for stage in (pipeline_def or {}).get("stages", []) or []:
        for action in stage.get("actions", []) or []:
            cfg = action.get("configuration", {}) or {}
            for key in ("TemplatePath", "TemplateConfiguration"):
                path = cfg.get(key) or ""
                _, _, file_path = path.partition("::")
                first = file_path.split("/", 1)[0]
                if first.startswith("assembly-"):
                    required.add(first)
    return required


def _select_cdk_out(candidates, required):
    """Choose the cdk.out among candidates that holds the most required
    assembly dirs. Falls back to the first candidate when nothing matches (or
    no assemblies are required), preserving the original single-app behavior.
    Returns (chosen_path, matched_count)."""
    valid = [c for c in candidates if c.exists() and (c / "manifest.json").exists()]
    if not valid:
        return None, 0
    if not required:
        return valid[0], 0
    best, best_score = valid[0], -1
    for c in valid:
        score = sum(1 for a in required if (c / a).is_dir())
        if score > best_score:
            best, best_score = c, score
    return best, max(best_score, 0)


def _zip_directory(src: Path, pipeline_name: str = "", required_assemblies=None) -> bytes:
    buf = io.BytesIO()
    # CDK Pipelines deploy actions reference templates by paths like
    # `assembly-X/foo.template.json` — those live inside cdk.out. To make
    # path-B (pre-synthed source) work, use cdk.out as the artifact root.
    # Common locations: <repo>/cdk.out, <repo>/apps/*/cdk.out, env override.
    # Resolution order: per-pipeline map > global override > assembly-matched
    # auto-detect. The map/override are explicit escape hatches; normally the
    # auto-detect picks the cdk.out that actually holds the pipeline's stacks.
    pipeline_subdir = _pipeline_cdk_out_subdir(pipeline_name)
    override = os.environ.get("MINISTACK_CONNECTION_CDK_OUT_PATH")
    if pipeline_subdir:
        root = src / pipeline_subdir
        logger.info("Source: pipeline %s mapped to %s as artifact root",
                    pipeline_name, root)
    elif override:
        root = Path(override)
        logger.info("Source: using override %s as artifact root", root)
    else:
        candidates = [src / "cdk.out"] + list(src.glob("apps/*/cdk.out")) + list(src.glob("*/cdk.out"))
        cdk_out, matched = _select_cdk_out(candidates, required_assemblies)
        if cdk_out:
            root = cdk_out
            if required_assemblies and matched:
                logger.info("Source: pipeline %s auto-matched %d/%d assemblies in %s",
                            pipeline_name, matched, len(required_assemblies), root)
            elif required_assemblies:
                # We know which assemblies the pipeline needs, but no candidate
                # cdk.out contains any of them — the synth is missing/stale.
                logger.warning("Source: pipeline %s needs assemblies %s but none of "
                               "%d cdk.out candidates contain them; using %s "
                               "(deploy will likely fail to find the template)",
                               pipeline_name, sorted(required_assemblies),
                               len(candidates), root)
            else:
                logger.info("Source: using pre-synthed %s as artifact root", root)
        else:
            root = src
            logger.info("Source: using full repo %s as artifact root (no cdk.out present)", root)
    # When the artifact root is a cdk.out directory, only include the
    # CloudFormation templates + manifests — skip bundled lambda asset dirs
    # (51k+ files, ~500 MB) since deploy actions only need template data.
    # Use os.walk so we can prune subtrees, not just filter individual files.
    is_cdk_out = (root / "manifest.json").exists()
    skip_top = {"node_modules", ".git", ".turbo", "dist", "tsconfig.tsbuildinfo"}
    root_str = str(root)
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for dirpath, dirnames, filenames in os.walk(root_str):
            rel_dir = Path(dirpath).relative_to(root_str)
            # Prune well-known directories at any depth
            if is_cdk_out:
                dirnames[:] = [d for d in dirnames if not d.startswith("asset.")]
            else:
                if str(rel_dir) == ".":
                    dirnames[:] = [d for d in dirnames if d not in skip_top]
            for fname in filenames:
                full = Path(dirpath) / fname
                rel = full.relative_to(root_str)
                if is_cdk_out and not (fname.endswith(".json") or fname.endswith(".dot") or fname == "manifest.json"):
                    continue
                try:
                    zf.write(full, rel.as_posix())
                except (OSError, PermissionError):
                    continue
    return buf.getvalue()


async def _action_source_s3(action, artifacts):
    """Source from S3 (CDK pipelines sometimes use this). Out of scope for v1."""
    logger.warning("S3 source action not implemented; producing empty artifact")
    for art in action.get("outputArtifacts", []):
        artifacts[art["name"]] = b""


# ---------------------------------------------------------------------------
# Build / Test action — Tier 2 simple pass-through (no Docker)
# ---------------------------------------------------------------------------

async def _action_build_codebuild_docker(action, artifacts, pipeline_def=None):
    """Tier 3c — run the CodeBuild project's buildspec in a real Docker container.

    Mounts the project's bind-mounted source as /codebuild/output/src so the
    container reads from and writes to the host working tree (allows the
    synthed cdk.out to be picked up by subsequent asset-publish actions).
    Build environment image defaults to aws/codebuild/standard:7.0 (matching
    CDK Pipelines codeBuildDefaults).

    Opt-in: only fires when MINISTACK_CODEPIPELINE_REAL_BUILD=1 and the
    action's CodeBuild project has a real buildspec.
    """
    config = action.get("configuration", {})
    project_name = config.get("ProjectName") or "(unknown)"
    action_name = action.get("name", "")

    # Look up the project record (created at CFN-deploy time) for buildspec.
    import ministack.services.codebuild as _codebuild_svc
    project = _codebuild_svc._projects.get(project_name)
    if not project:
        logger.info("CodeBuild docker: project %s not found; falling back to pass-through", project_name)
        return False

    source = project.get("source") or {}
    buildspec = source.get("buildspec") or config.get("BuildSpec")
    if not buildspec:
        logger.info("CodeBuild docker: no buildspec for %s; pass-through", project_name)
        return False

    # Extract phase commands from the buildspec YAML
    import yaml
    try:
        bs = yaml.safe_load(buildspec)
    except yaml.YAMLError as e:
        logger.warning("CodeBuild docker: buildspec parse error for %s: %s", project_name, e)
        return False

    phases = (bs.get("phases") or {})
    commands: list[str] = []
    for phase in ("install", "pre_build", "build", "post_build"):
        ph = phases.get(phase) or {}
        for cmd in ph.get("commands", []) or []:
            commands.append(cmd)
    if not commands:
        logger.info("CodeBuild docker: no commands for %s; pass-through", project_name)
        return False

    # Build image — fall back to the CDK Pipelines default if not specified.
    env = project.get("environment") or {}
    image = env.get("image") or "public.ecr.aws/codebuild/standard:7.0"

    src_host = os.environ.get("MINISTACK_CONNECTION_DEFAULT_PATH")
    if not src_host:
        logger.warning("CodeBuild docker: no source bind-mount available")
        return False

    work_subdir = config.get("PrimarySource") or "."
    container_workdir = "/codebuild/output/src"

    # Compose env vars for the build container — give it AWS endpoint, region,
    # account so cdk-assets / aws-sdk inside the build target MiniStack.
    target_account = (
        _parse_account_from_arn(action.get("roleArn", ""))
        or os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
    )
    target_region = action.get("region") or os.environ.get("MINISTACK_REGION", "us-east-1")
    docker_endpoint = os.environ.get(
        "MINISTACK_BUILD_ENDPOINT_URL", "http://host.docker.internal:4566"
    )

    env_args = [
        "-e", f"AWS_ACCESS_KEY_ID={target_account}",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", f"AWS_DEFAULT_REGION={target_region}",
        "-e", f"AWS_REGION={target_region}",
        "-e", f"AWS_ENDPOINT_URL={docker_endpoint}",
        "-e", f"AWS_ENDPOINT_URL_S3={docker_endpoint}",
        "-e", f"CODEBUILD_BUILD_ID={project_name}:{_codepipeline._now_iso()}",
        "-e", f"CODEBUILD_SRC_DIR={container_workdir}",
    ]
    for k, v in (env.get("environmentVariables") or []):
        env_args += ["-e", f"{k}={v}"]
    # Allow overriding the build-network so the container can resolve
    # `ministack` if running on the same compose network.
    network = os.environ.get("MINISTACK_BUILD_DOCKER_NETWORK", "")
    network_args = ["--network", network] if network else []

    script = " && ".join(commands)
    logger.info("CodeBuild docker: launching %s (image=%s, %d commands)",
                project_name, image, len(commands))

    proc = await asyncio.create_subprocess_exec(
        "docker", "run", "--rm",
        "-v", f"{src_host}:{container_workdir}:rw",
        *env_args, *network_args,
        "-w", container_workdir,
        image,
        "bash", "-c", script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    deadline = int(os.environ.get("MINISTACK_BUILD_TIMEOUT_SEC", "1800"))
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=deadline)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError(f"CodeBuild docker: {project_name} timed out after {deadline}s")

    text = stdout.decode("utf-8", errors="replace")
    for line in text.splitlines()[-30:]:
        logger.info("[%s] %s", project_name, line)

    if proc.returncode != 0:
        raise RuntimeError(f"CodeBuild docker: {project_name} exited {proc.returncode}")

    # Output artifact: just pass-through the input (real CodeBuild would
    # collect output files based on the buildspec artifacts: declaration).
    inputs = action.get("inputArtifacts") or []
    outputs = action.get("outputArtifacts") or []
    payload = artifacts.get(inputs[0]["name"]) if inputs and inputs[0]["name"] in artifacts else b""
    for art in outputs:
        artifacts[art["name"]] = payload
    return True


async def _action_build_codebuild(action, artifacts, pipeline_def=None):
    """Build action handler.

    Behaviour matrix:
    - "Synth" actions (UpdatePipeline / Build/Synth): pass-through, since the
      source artifact already IS the synthed cdk.out (path B). Future Tier 3
      would run the buildspec in Docker.
    - "FileAsset*" / "DockerAsset*" actions (the Assets stage): walk the input
      artifact's *.assets.json manifests and upload each file asset to the
      destination S3 bucket / region / account. This makes Lambda code zips,
      template assets, etc. actually retrievable from S3 at deploy time.
    """
    config = action.get("configuration", {})
    project_name = config.get("ProjectName") or "(unknown)"
    inputs = action.get("inputArtifacts") or []
    outputs = action.get("outputArtifacts") or []
    action_name = action.get("name", "")

    payload = b""
    if inputs and inputs[0]["name"] in artifacts:
        payload = artifacts[inputs[0]["name"]]

    # Detect "publish assets" actions by name convention. CDK Pipelines names
    # them FileAsset1, FileAsset2, ..., DockerAsset1, ... (see
    # @aws-cdk/pipelines).
    is_asset_publish = (action_name.startswith("FileAsset")
                        or action_name.startswith("DockerAsset"))

    # Tier 3c — opt-in real Docker CodeBuild execution. Skips asset-publish
    # (Tier 3a already covers them) and SelfMutate (Tier 3b covers it).
    if (os.environ.get("MINISTACK_CODEPIPELINE_REAL_BUILD") == "1"
            and not is_asset_publish
            and action_name != "SelfMutate"):
        try:
            ran = await _action_build_codebuild_docker(action, artifacts, pipeline_def)
            if ran:
                return
        except Exception:
            logger.exception("Real CodeBuild failed for %s; falling back to pass-through",
                             action_name)

    if is_asset_publish:
        # Asset publish walks ALL manifests, so we only need to run it once
        # per pipeline execution. Subsequent FileAssetN actions short-circuit.
        if "__assets_published__" not in artifacts:
            try:
                published = await asyncio.to_thread(_publish_assets_from_disk)
                logger.info("Asset action %s published %d objects from disk",
                            action_name, published)
                artifacts["__assets_published__"] = b"1"
            except Exception:
                logger.exception("Asset publish for %s failed; continuing", action_name)
        else:
            logger.info("Asset action %s: assets already published this run, skipping",
                        action_name)

    # SelfMutate action — UpdatePipeline stage. CDK Pipelines emits this so
    # the pipeline can re-deploy itself when the pipeline definition changes.
    # Real CodeBuild would `cdk deploy <PipelineStack>` against the latest
    # synth. We approximate it by re-applying the current synthed template
    # (opt-in via MINISTACK_CODEPIPELINE_SELF_MUTATE=1).
    if action_name == "SelfMutate" and _self_mutate_enabled():
        try:
            updated = await _self_mutate(pipeline_def)
            logger.info("SelfMutate: re-deployed %s", updated or "(nothing)")
        except Exception:
            logger.exception("SelfMutate failed; continuing")

    logger.info("Build action %s: pass-through (%d bytes) -> %s",
                project_name, len(payload), [o["name"] for o in outputs])
    for art in outputs:
        artifacts[art["name"]] = payload


def _publish_assets_from_disk() -> int:
    """Walk *.assets.json under the connection's cdk.out and upload each asset.

    Replaces what `cdk-assets publish` does in real CodeBuild. Reads source
    files directly from the host bind-mount (instead of from the pipeline
    artifact zip), so we avoid having to embed gigabytes of bundled lambda
    code in the pipeline artifact.

    Efficiency (matches real cdk-assets behaviour):
      - HeadObject existence check BEFORE zipping. CDK asset keys are
        content-hashed, so an object already in S3 is always current — skip it
        entirely (no zip, no upload). This makes repeat runs near-instant.
      - One cached S3 client per (account, region), not one per asset.
      - Zip + upload the cache-misses in parallel via a thread pool.
      - ZIP_STORED (no compression): CPU is the bottleneck locally, the
        MiniStack S3 hop is cheap, so skipping deflate is a net win.

    Returns the number of objects actually uploaded (skips don't count).
    """
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from concurrent.futures import ThreadPoolExecutor

    endpoint = os.environ.get("AWS_ENDPOINT_URL_MINISTACK", "http://localhost:4566")
    base = _resolve_default_cdk_out()
    if not base:
        logger.warning("Asset publish: no cdk.out found at MINISTACK_CONNECTION_DEFAULT_PATH; skipping")
        return 0

    # Short timeouts + no real retries: these are loopback self-calls to a
    # single-process MiniStack. With boto3's 60s default read_timeout, one
    # head_object/put_object landing while the event loop is busy (cold-start
    # contention) stalls the whole asset pass for a minute. Same rationale as the
    # CFN-deploy client in _cfn_create_or_update_stack.
    _cfg = Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 1})

    # Cache S3 clients by (account, region).
    _clients: dict[tuple[str, str], object] = {}

    def _client(account, region):
        key = (account, region)
        c = _clients.get(key)
        if c is None:
            c = boto3.client(
                "s3", endpoint_url=endpoint, config=_cfg,
                aws_access_key_id=account, aws_secret_access_key="test",
                region_name=region,
            )
            _clients[key] = c
        return c

    max_workers = int(os.environ.get("MINISTACK_ASSET_PUBLISH_WORKERS", "8"))

    # 1. Collect candidate (client, bucket, key, src, packaging) tuples.
    # Manifests live at the cdk.out top level and in assembly-*/ subdirs, never
    # inside asset.*/ bundles. Prune those (and node_modules) so we don't
    # stat-walk the ~100k files of installed Lambda node_modules just to find a
    # few dozen manifests (mirrors the prune in _zip_directory).
    manifest_paths = []
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames
                       if not d.startswith("asset.") and d != "node_modules"]
        for fn in filenames:
            if fn.endswith(".assets.json"):
                manifest_paths.append(Path(dirpath) / fn)

    candidates = []
    for manifest_path in manifest_paths:
        try:
            manifest = json.loads(manifest_path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        for asset_hash, asset in (manifest.get("files") or {}).items():
            source = asset.get("source", {}) or {}
            source_path = source.get("path")
            packaging = source.get("packaging", "file")
            if not source_path:
                continue
            src_full = (manifest_path.parent / source_path).resolve()
            if not src_full.exists():
                continue
            for dest_key, dest in (asset.get("destinations") or {}).items():
                bucket = dest.get("bucketName")
                object_key = dest.get("objectKey")
                region = dest.get("region", os.environ.get("MINISTACK_REGION", "us-east-1"))
                if not bucket or not object_key:
                    continue
                account = _parse_account_from_arn(dest.get("assumeRoleArn", ""))
                if not account:
                    parts = dest_key.split("-", 1)
                    account = parts[0] if parts and re.match(r"^\d{12}$", parts[0]) else None
                if not account:
                    account = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
                candidates.append((_client(account, region), bucket, object_key, src_full, packaging))

    if not candidates:
        return 0

    # 2. Existence check in PARALLEL. Content-hashed keys => a hit is current, so
    #    skip the zip + upload. Parallel so N serial loopback head_objects don't
    #    dominate the stage on a cold run.
    def _needs_upload(item) -> bool:
        client, bucket, object_key, _src, _pkg = item
        try:
            client.head_object(Bucket=bucket, Key=object_key)
            return False  # present => current => skip
        except ClientError:
            return True   # 404 / NoSuchKey => upload
        except Exception:
            return True   # bucket missing etc. => let the put surface it

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        present = list(pool.map(_needs_upload, candidates))
    work = [c for c, needs in zip(candidates, present) if needs]
    skipped = len(candidates) - len(work)
    if skipped:
        logger.info("Asset publish: %d already present, skipping", skipped)
    if not work:
        return 0

    # Local-native mode (default): for directory ("zip"-packaged) assets —
    # which are almost always Lambda code bundles — upload a few-byte marker
    # pointing at the on-disk asset dir instead of zipping + uploading 30MB.
    # MiniStack's Lambda provisioner runs the function straight from that dir.
    # Set MINISTACK_LOCAL_LAMBDA_ASSETS=0 to fall back to real zip uploads.
    local_assets = os.environ.get("MINISTACK_LOCAL_LAMBDA_ASSETS", "1") != "0"

    # 3. Upload the misses in parallel.
    def _do(item) -> bool:
        client, bucket, object_key, src_full, packaging = item
        try:
            if packaging == "zip" and src_full.is_dir():
                if local_assets:
                    import ministack.services.lambda_svc as _lambda_svc
                    body = _lambda_svc.make_local_asset_marker(str(src_full))
                else:
                    body = _zip_dir_to_bytes(src_full)
            elif src_full.is_file():
                body = src_full.read_bytes()
            else:
                return False
            client.put_object(Bucket=bucket, Key=object_key, Body=body)
            return True
        except Exception:
            logger.exception("Asset publish failed -> s3://%s/%s", bucket, object_key)
            return False

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(_do, work))
    return sum(1 for r in results if r)


def _resolve_default_cdk_out():
    """Return the cdk.out Path for the default connection, or None."""
    src = os.environ.get("MINISTACK_CONNECTION_DEFAULT_PATH")
    if not src:
        return None
    p = Path(src)
    if not p.exists():
        return None
    candidates = [p / "cdk.out"] + list(p.glob("apps/*/cdk.out")) + list(p.glob("*/cdk.out"))
    for c in candidates:
        if c.exists() and (c / "manifest.json").exists():
            return c
    return None


def _zip_dir_to_bytes(directory: Path) -> bytes:
    """Zip a directory tree into in-memory bytes, mirroring `cdk-assets`.

    Uses ZIP_STORED (no compression): the asset is consumed locally by
    MiniStack's Lambda service over a fast loopback hop, so deflate CPU cost
    isn't worth the bandwidth saving. Lambda code zips are mostly already-
    minified JS + node_modules that compress poorly anyway.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        for path in directory.rglob("*"):
            if path.is_dir():
                continue
            rel = path.relative_to(directory)
            try:
                zf.write(path, rel.as_posix())
            except (OSError, PermissionError):
                continue
    return buf.getvalue()


def _continue_on_error() -> bool:
    return os.environ.get("MINISTACK_CODEPIPELINE_CONTINUE_ON_ERROR", "0") == "1"


def _self_mutate_enabled() -> bool:
    return os.environ.get("MINISTACK_CODEPIPELINE_SELF_MUTATE", "0") == "1"


async def _self_mutate(pipeline_def):
    """Re-deploy the pipeline's own CloudFormation stack from the latest synth.

    CDK Pipelines emits a SelfMutate stage that, in real AWS, runs
    `cdk deploy <PipelineStack>` to update the pipeline definition when its
    source changes. We approximate by reading the synthed template from
    cdk.out and calling CFN UpdateStack on the pipeline's owning stack.

    Stack identification: CDK names the pipeline stack the same as the
    AWS::CodePipeline::Pipeline resource's logical id in main.ts — for
    a pipeline with logical id `FooPipeline`, the stack name is "Foo". We resolve
    by walking cdk.out manifests for a stack whose template contains a
    Pipeline resource with the same name.
    """
    cdk_out = _resolve_default_cdk_out()
    if not cdk_out:
        return None

    target_pipeline_name = (pipeline_def or {}).get("name", "")
    stack_name, template_body = _find_pipeline_stack_template(cdk_out, target_pipeline_name)
    if not stack_name or not template_body:
        logger.warning("SelfMutate: could not find pipeline stack for %s", target_pipeline_name)
        return None

    # Pipeline stack lives in whichever account the pipeline itself runs in.
    # We resolve from the StackArtifact's environment in manifest.json.
    account, region = _find_stack_environment(cdk_out, stack_name)
    region = region or os.environ.get("MINISTACK_REGION", "us-east-1")
    account = account or os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")

    await _cfn_create_or_update_stack(stack_name, template_body, {}, region, account)
    return stack_name


def _find_pipeline_stack_template(cdk_out: Path, pipeline_name: str):
    """Find the synthed template whose pipeline name matches."""
    for tpl in cdk_out.glob("*.template.json"):
        try:
            obj = json.loads(tpl.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        for res in (obj.get("Resources") or {}).values():
            if res.get("Type") != "AWS::CodePipeline::Pipeline":
                continue
            if (res.get("Properties") or {}).get("Name") == pipeline_name:
                return tpl.stem.replace(".template", ""), tpl.read_text()
    return None, None


def _find_stack_environment(cdk_out: Path, stack_name: str):
    """Read cdk.out/manifest.json to pull the account/region for a stack."""
    manifest_path = cdk_out / "manifest.json"
    if not manifest_path.exists():
        return None, None
    try:
        manifest = json.loads(manifest_path.read_text())
    except (json.JSONDecodeError, OSError):
        return None, None
    artifacts = manifest.get("artifacts") or {}
    art = artifacts.get(stack_name) or {}
    env = art.get("environment") or ""
    # CDK format: aws://<account>/<region>
    m = re.match(r"aws://(\d{12})/([\w-]+)", env)
    if m:
        return m.group(1), m.group(2)
    return None, None


def _parse_account_from_arn(arn: str):
    if not arn or ":iam::" not in arn:
        return None
    try:
        return arn.split(":iam::", 1)[1].split(":", 1)[0]
    except (IndexError, AttributeError):
        return None



# ---------------------------------------------------------------------------
# Deploy CloudFormation action
# ---------------------------------------------------------------------------

async def _action_deploy_cfn(action, artifacts, pipeline_def):
    config = action.get("configuration", {})
    stack_name = config.get("StackName")
    if not stack_name:
        logger.warning("Deploy CFN: missing StackName, skipping")
        return

    action_mode = config.get("ActionMode", "CREATE_UPDATE")
    template_path = config.get("TemplatePath", "")
    template_config_path = config.get("TemplateConfiguration", "")

    if action_mode == "CHANGE_SET_REPLACE":
        # CDK Pipelines uses CHANGE_SET_REPLACE (Prepare) then
        # CHANGE_SET_EXECUTE. For Tier 2 we collapse both into a single direct
        # create/update.
        action_mode = "CREATE_UPDATE"

    if action_mode == "CHANGE_SET_EXECUTE":
        # We already did the work on the CHANGE_SET_REPLACE pass.
        logger.info("Deploy CFN: CHANGE_SET_EXECUTE for %s -> already done", stack_name)
        return

    if action_mode == "DELETE_ONLY":
        await _cfn_delete_stack(stack_name)
        return

    if not template_path:
        logger.warning("Deploy CFN: no TemplatePath for %s, skipping", stack_name)
        return

    # Resolve the input artifact + path inside it.
    artifact_name, _, file_path = template_path.partition("::")
    blob = artifacts.get(artifact_name)
    if not blob:
        raise RuntimeError(f"Artifact {artifact_name} not present in pipeline artifacts")

    template_body = _read_from_zip(blob, file_path)
    if template_body is None:
        # CDK sometimes puts assembly artifacts in subdirs (assembly-Foo/...);
        # fall back to a fuzzy match.
        template_body = _read_from_zip_fuzzy(blob, file_path)
        if template_body is None:
            raise RuntimeError(f"Template {file_path} not found in artifact {artifact_name}")

    # Parameters: from TemplateConfiguration JSON if specified.
    parameters = {}
    if template_config_path:
        cfg_artifact, _, cfg_file = template_config_path.partition("::")
        cfg_blob = artifacts.get(cfg_artifact) or blob
        cfg_body = _read_from_zip(cfg_blob, cfg_file)
        if cfg_body:
            try:
                cfg = json.loads(cfg_body)
                parameters = cfg.get("Parameters", {})
            except json.JSONDecodeError:
                logger.warning("Could not parse TemplateConfiguration JSON for %s", stack_name)

    # Region from the action's region attribute (CDK Pipelines sets this for
    # cross-region deploys). Account from action.RoleArn if present.
    target_region = action.get("region") or os.environ.get("MINISTACK_REGION", "us-east-1")
    role_arn = action.get("roleArn") or ""
    target_account = _account_from_role_arn(role_arn) or os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")

    await _cfn_create_or_update_stack(stack_name, template_body, parameters,
                                       target_region, target_account)


def _account_from_role_arn(arn):
    if not arn:
        return None
    parts = arn.split(":")
    return parts[4] if len(parts) >= 5 else None


def _read_from_zip(blob, file_path):
    try:
        with zipfile.ZipFile(io.BytesIO(blob), "r") as zf:
            return zf.read(file_path)
    except (KeyError, zipfile.BadZipFile):
        return None


def _read_from_zip_fuzzy(blob, file_path):
    """Find file_path as a suffix of any entry in the zip."""
    try:
        with zipfile.ZipFile(io.BytesIO(blob), "r") as zf:
            for name in zf.namelist():
                if name.endswith(file_path):
                    return zf.read(name)
    except zipfile.BadZipFile:
        return None
    return None


# ---------------------------------------------------------------------------
# CFN bridge — call MiniStack's own CloudFormation engine from inside.
# ---------------------------------------------------------------------------

async def _cfn_create_or_update_stack(stack_name, template_body, parameters,
                                       region, account_id):
    """Create or update a CFN stack on this same MiniStack instance.

    We call boto3 over localhost rather than poking the engine directly so we
    re-use the existing template parsing / dependency-resolution code path,
    and so account/region routing happens via standard request-context vars.
    """
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("AWS_ENDPOINT_URL_MINISTACK", "http://localhost:4566")
    # Short timeouts: this client calls MiniStack's CFN over HTTP back into
    # the SAME process. While a deploy task briefly holds the event loop, a
    # describe_stacks call can't be serviced and would otherwise hang until
    # boto3's default 60s read_timeout — turning a 0.5s deploy into a 60s
    # poll cycle. A 3s read timeout means a blocked poll retries in seconds.
    cfg = Config(
        retries={"max_attempts": 0},
        connect_timeout=2,
        read_timeout=3,
    )
    session = boto3.session.Session(
        aws_access_key_id=account_id, aws_secret_access_key="test", region_name=region
    )
    client = session.client("cloudformation", endpoint_url=endpoint, config=cfg)

    cfn_params = [{"ParameterKey": k, "ParameterValue": str(v)} for k, v in (parameters or {}).items()]
    body = template_body.decode() if isinstance(template_body, (bytes, bytearray)) else template_body

    status = None
    try:
        existing = client.describe_stacks(StackName=stack_name)
        stacks = existing.get("Stacks") or []
        if stacks:
            status = stacks[0].get("StackStatus", "")
    except Exception as e:
        # ValidationError "does not exist" => need to create. Any other
        # exception is a transient/setup problem and we should still try.
        if "does not exist" not in str(e):
            logger.warning("describe_stacks(%s) raised: %s", stack_name, e)

    capabilities = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"]

    def _is_landed_timeout(msg):
        # The create/update request reaches MiniStack synchronously (the handler
        # schedules the deploy before responding); under event-loop contention
        # the *response* can miss the short read_timeout even though the
        # operation already landed server-side. Treat these as "landed" and let
        # the poll loop below confirm the real terminal state — genuine failures
        # still surface there as FAILED/ROLLBACK.
        low = msg.lower()
        return "readtimeout" in low or "read timeout" in low or "timed out" in low

    def _do_create():
        client.create_stack(
            StackName=stack_name, TemplateBody=body, Parameters=cfn_params,
            Capabilities=capabilities,
        )

    def _do_update():
        try:
            client.update_stack(
                StackName=stack_name, TemplateBody=body, Parameters=cfn_params,
                Capabilities=capabilities,
            )
        except Exception as e:
            msg = str(e)
            if "No updates are to be performed" in msg:
                logger.info("CFN no-op update for %s", stack_name)
                return
            if _is_landed_timeout(msg):
                logger.info("CFN update %s timed out on response; polling for stack", stack_name)
                return
            raise

    if status is None or status in ("DELETE_COMPLETE", "ROLLBACK_COMPLETE"):
        try:
            await asyncio.to_thread(_do_create)
        except Exception as e:
            msg = str(e)
            if "already exists" in msg:
                # describe-stacks missed (or timed out) but the stack exists.
                # Update instead so re-running the pipeline is idempotent. The
                # update is itself timeout-tolerant via _do_update.
                logger.info("CFN create %s collided; falling back to update", stack_name)
                await asyncio.to_thread(_do_update)
            elif _is_landed_timeout(msg):
                logger.info("CFN create %s timed out on response; polling for stack", stack_name)
            else:
                raise
    else:
        await asyncio.to_thread(_do_update)

    # Poll for completion. Some stacks (Cognito user pools, KMS, etc.) take
    # longer than the obvious "no real cloud, should be fast" intuition —
    # MiniStack still walks the resource graph synchronously and some
    # provisioners (RDS, Cognito) are slow. 5 minute deadline gives headroom.
    deadline = time.time() + int(os.environ.get("MINISTACK_CFN_DEPLOY_TIMEOUT_SEC", "300"))
    while time.time() < deadline:
        try:
            r = await asyncio.to_thread(lambda: client.describe_stacks(StackName=stack_name))
            st = r["Stacks"][0].get("StackStatus", "")
            if st.endswith("_COMPLETE") and not st.endswith("ROLLBACK_COMPLETE"):
                logger.info("CFN deploy %s finished: %s", stack_name, st)
                return
            if "FAILED" in st or "ROLLBACK_COMPLETE" in st:
                raise RuntimeError(f"CFN deploy {stack_name} ended in {st}")
        except RuntimeError:
            raise
        except Exception as e:
            if "FAILED" in str(e) or "ROLLBACK" in str(e):
                raise
            # transient describe error during create — continue polling
        await asyncio.sleep(1.0)
    raise RuntimeError(f"CFN deploy {stack_name} timed out")


async def _cfn_delete_stack(stack_name):
    import boto3
    endpoint = os.environ.get("AWS_ENDPOINT_URL_MINISTACK", "http://localhost:4566")
    client = boto3.client("cloudformation", endpoint_url=endpoint, region_name=os.environ.get("MINISTACK_REGION", "us-east-1"))

    def _do():
        client.delete_stack(StackName=stack_name)

    await asyncio.to_thread(_do)


# ---------------------------------------------------------------------------
# Lambda invoke action (stub)
# ---------------------------------------------------------------------------

async def _action_invoke_lambda(action, artifacts):
    logger.info("Lambda invoke action — stubbed (succeeded)")


# ---------------------------------------------------------------------------
# Register with codepipeline if the runtime is enabled.
# ---------------------------------------------------------------------------

if os.environ.get("MINISTACK_CODEPIPELINE_RUNTIME", "1") != "0":
    _codepipeline.set_executor(_run_pipeline)
    logger.info("CodePipeline runtime registered (Tier 2 orchestrator)")
else:
    logger.info("CodePipeline runtime disabled via MINISTACK_CODEPIPELINE_RUNTIME=0")
