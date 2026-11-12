import json
import time


def _wait_stack(cfn, name, timeout=30):
    """Poll until stack reaches terminal status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        stacks = cfn.describe_stacks(StackName=name)["Stacks"]
        status = stacks[0]["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return stacks[0]
        time.sleep(0.5)
    raise TimeoutError(f"Stack {name} stuck at {status}")


def _template(memory):
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            # No explicit FunctionName: the physical name is auto-derived from
            # the stack name + logical id. This is exactly the case that used to
            # get the stack name re-prepended on every UPDATE.
            "Func": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "Runtime": "python3.12",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::000000000000:role/dummy-role",
                    "MemorySize": memory,
                    "Code": {"ZipFile": "def handler(e, c): return {}"},
                },
            },
        },
    }


def test_cfn_lambda_update_preserves_name_and_arn(cfn, lam):
    """An auto-named Lambda keeps its physical name + ARN across stack updates.

    Regression: AWS::Lambda::Function had no update handler, so each UPDATE
    re-prepended the stack name to the existing physical id and truncated to
    64 chars. After a few updates every auto-named Lambda collapsed onto one
    name, overwriting each other and breaking custom-resource ServiceToken
    wiring (the GetAtt Arn resolved to the wrong function).
    """
    stack_name = "cfn-lam-upd"

    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(_template(128)))
    _wait_stack(cfn, stack_name)

    def func_resource():
        resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
        return next(r for r in resources if r["ResourceType"] == "AWS::Lambda::Function")

    original_name = func_resource()["PhysicalResourceId"]
    original_arn = lam.get_function(FunctionName=original_name)["Configuration"]["FunctionArn"]

    # Two consecutive updates, each mutating a mutable property (MemorySize).
    for memory in (256, 512):
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(_template(memory)))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE"

        name = func_resource()["PhysicalResourceId"]
        config = lam.get_function(FunctionName=name)["Configuration"]

        # Name + ARN must be stable across the update.
        assert name == original_name
        assert config["FunctionArn"] == original_arn
        # The mutated property actually took effect.
        assert config["MemorySize"] == memory
        # The stack name was never double-prefixed onto the physical name.
        assert name.count(stack_name) == 1
        # The function is still resolvable by its name in the lambda service.
        assert config["FunctionName"] == original_name
