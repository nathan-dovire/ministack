"""Startup hook: pre-bootstrap CDK accounts from MINISTACK_BOOTSTRAP_ACCOUNTS.

Real CDK requires `cdk bootstrap` per account-region pair before any `cdk
deploy` works (it checks `/cdk-bootstrap/hnb659fds/version` and bails on
missing/old). Locally this is the same slow setup, repeated whenever a project
spins up MiniStack fresh — every project's deploy script ends up implementing
the same SSM-check + `cdk bootstrap` loop.

This module turns that into a startup hook. Set MINISTACK_BOOTSTRAP_ACCOUNTS
to a comma-separated list of 12-digit account IDs; MiniStack writes the SSM
parameter and creates the CDK assets bucket per account, so first-run
`cdk deploy` against MiniStack just works without any per-project bootstrap
plumbing.

What it provisions per account (in MINISTACK_REGION):
  - SSM /cdk-bootstrap/hnb659fds/version = "32"
  - S3  cdk-hnb659fds-assets-<account>-<region>

That's the subset cdklocal actually inspects + reads from. Roles and ECR repos
in the real CDKToolkit stack aren't enforced locally.

Idempotent: re-runs are no-ops (SSM PutParameter with Overwrite=True, S3
CreateBucket is idempotent for the same account).
"""

from __future__ import annotations

import logging
import os

from ministack.core.responses import _request_account_id, _request_region

logger = logging.getLogger(__name__)

_BOOTSTRAP_VERSION = "32"


def bootstrap_cdk_accounts() -> None:
    raw = os.environ.get("MINISTACK_BOOTSTRAP_ACCOUNTS", "").strip()
    if not raw:
        return

    accounts = [a.strip() for a in raw.split(",") if a.strip()]
    if not accounts:
        return

    region = os.environ.get("MINISTACK_REGION") or os.environ.get("AWS_REGION") or "us-east-1"

    # Lazy imports: avoid pulling these in at module-load time so unit tests
    # for unrelated services don't drag in S3/SSM state.
    from ministack.services import s3 as _s3
    from ministack.services import ssm as _ssm

    for account in accounts:
        tok_a = _request_account_id.set(account)
        tok_r = _request_region.set(region)
        try:
            _put_bootstrap_parameter(_ssm)
            _create_assets_bucket(_s3, account, region)
            logger.info("CDK bootstrap: aws://%s/%s (ssm=v%s, bucket=cdk-hnb659fds-assets-%s-%s)",
                        account, region, _BOOTSTRAP_VERSION, account, region)
        except Exception:
            logger.exception("CDK bootstrap failed for aws://%s/%s", account, region)
        finally:
            _request_region.reset(tok_r)
            _request_account_id.reset(tok_a)


def _put_bootstrap_parameter(ssm_mod) -> None:
    # Account scope comes from the contextvars the caller set; the SSM parameter
    # name is region-agnostic, so neither account nor region is needed here.
    # Write directly to the account-scoped store so we don't go through the
    # HTTP-shaped handler (which would require building a request envelope).
    name = "/cdk-bootstrap/hnb659fds/version"
    existing = ssm_mod._parameters.get(name)
    if existing and existing.get("Value") == _BOOTSTRAP_VERSION:
        return
    from ministack.services.ssm import _now_epoch, _param_arn
    record = {
        "Name": name,
        "Value": _BOOTSTRAP_VERSION,
        "OriginalValue": _BOOTSTRAP_VERSION,
        "Type": "String",
        "KeyId": "",
        "Version": (existing["Version"] + 1) if existing else 1,
        "ARN": _param_arn(name),
        "LastModifiedDate": _now_epoch(),
        "DataType": "text",
        "Description": "CDK bootstrap version (pre-seeded by MiniStack startup hook)",
        "Tier": "Standard",
        "AllowedPattern": "",
        "Policies": [],
        "Labels": [],
    }
    ssm_mod._parameters[name] = record


def _create_assets_bucket(s3_mod, account: str, region: str) -> None:
    bucket = f"cdk-hnb659fds-assets-{account}-{region}"
    if bucket in s3_mod._buckets:
        return
    from ministack.services.s3 import now_iso
    s3_mod._buckets[bucket] = {"created": now_iso(), "objects": {}, "region": region}
