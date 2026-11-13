"""Fidelity: Secrets Manager.

Each test checks one documented AWS behaviour, quoted in its docstring, and that the response on the
wire fits the operation's model.
"""
import json
import os
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from fidelity.fidelity_model import WireRecorder

OWNER, CALLER = "111122223333", "000000000000"
GET_SECRET_VALUE = "https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html"
CROSS_ACCOUNT = "https://docs.aws.amazon.com/secretsmanager/latest/userguide/auth-and-access_examples_cross.html"


def _client(service, account):
    return boto3.client(service, endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
                        region_name="us-east-1", aws_access_key_id=account, aws_secret_access_key="test")


@pytest.fixture
def owned_secret():
    owner = _client("secretsmanager", OWNER)
    name = f"fid-xacct-{uuid.uuid4().hex[:8]}"
    arn = owner.create_secret(Name=name, SecretString="shared")["ARN"]
    yield name, arn
    owner.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


def _read(secret_id):
    client = _client("secretsmanager", CALLER)
    wire = WireRecorder(client)
    try:
        result = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        result = exc.response["Error"]["Code"]
    assert wire.problems() == []
    return result


def test_complete_arn_reads_the_secret_in_the_owning_account(owned_secret):
    """GetSecretValue, SecretId: "To retrieve a secret from another account, you must use an ARN."
    (GET_SECRET_VALUE). With AUTH off no grant is checked, so the ARN alone must reach the secret."""
    name, arn = owned_secret
    result = _read(arn)
    assert (result["ARN"], result["Name"], result["SecretString"]) == (arn, name, "shared")


def test_partial_arn_reads_the_secret_in_the_owning_account(owned_secret):
    """GetSecretValue, SecretId: "For an ARN, we recommend that you specify a complete ARN rather than a
    partial ARN." (GET_SECRET_VALUE). A partial ARN is still an ARN, so it names the owning account."""
    _name, arn = owned_secret
    assert _read(arn.rsplit("-", 1)[0])["ARN"] == arn


def test_name_does_not_reach_another_account(owned_secret):
    """GetSecretValue, SecretId: "To retrieve a secret from another account, you must use an ARN."
    (GET_SECRET_VALUE). A name is looked up in the caller's account only."""
    name, _arn = owned_secret
    assert _read(name) == "ResourceNotFoundException"


def test_secret_in_another_account_scheduled_for_deletion_is_refused(owned_secret):
    """GetSecretValue errors, InvalidRequestException: "A parameter value is not valid for the current
    state of the resource. Possible causes: The secret is scheduled for deletion." (GET_SECRET_VALUE)"""
    name, arn = owned_secret
    _client("secretsmanager", OWNER).delete_secret(SecretId=name, RecoveryWindowInDays=7)
    assert _read(arn) == "InvalidRequestException"


@pytest.mark.auth_contract
def test_cross_account_read_needs_a_resource_policy_and_a_customer_managed_key(auth_enabled):
    """"To allow users in one account to access secrets in another account (cross-account access), you
    must allow access both in a resource policy and in an identity policy." and "you can't use the AWS
    managed key (aws/secretsmanager) for cross-account access" (CROSS_ACCOUNT). The caller is its
    account's root, so its identity policy allows; the resource policy and the key decide."""
    if not auth_enabled:
        pytest.skip("AWS grant rules apply only with AUTH=true")
    owner_sm, owner_kms = _client("secretsmanager", OWNER), _client("kms", OWNER)
    reader = {"AWS": f"arn:aws:iam::{CALLER}:root"}
    key = owner_kms.create_key(Policy=json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{OWNER}:root"}, "Action": "kms:*", "Resource": "*"},
        {"Effect": "Allow", "Principal": reader, "Action": "kms:Decrypt", "Resource": "*"}]}))["KeyMetadata"]["Arn"]
    policy = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": reader, "Action": "secretsmanager:GetSecretValue", "Resource": "*"}]})

    def read(with_policy, key_id):
        name = f"fid-xacct-grant-{uuid.uuid4().hex[:8]}"
        arn = owner_sm.create_secret(Name=name, SecretString="v", **({"KmsKeyId": key_id} if key_id else {}))["ARN"]
        if with_policy:
            owner_sm.put_resource_policy(SecretId=name, ResourcePolicy=policy)
        try:
            result = _read(arn)
            return result if isinstance(result, str) else result["SecretString"]
        finally:
            owner_sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    assert read(False, key) == "AccessDeniedException"
    assert read(True, None) == "AccessDeniedException"
    assert read(True, key) == "v"
