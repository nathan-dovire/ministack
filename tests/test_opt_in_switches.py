"""
Opt-in env switches that deviate from AWS behaviour. Each switch is read at
call time, so these tests flip it per test with monkeypatch and call the
module directly (the shared test server is not involved).
"""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

_MOCK_DATA = json.dumps({"SecretKey": "mock-secret", "value": "mock-value"})


@pytest.fixture
def response_url_server():
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            received.append(json.loads(body))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/cb", received
    server.shutdown()
    server.server_close()


def _cfn_event(url):
    return {
        "RequestType": "Create",
        "RequestId": "req-1",
        "StackId": "stack-1",
        "LogicalResourceId": "CR",
        "ResponseURL": url,
        "ResourceProperties": {"ServiceToken": "x", "Name": "n"},
    }


# ---------------------------------------------------------------------------
# MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS
# ---------------------------------------------------------------------------

def test_no_code_lambda_does_not_post_success_by_default(response_url_server, monkeypatch):
    from ministack.services import lambda_svc
    url, received = response_url_server
    monkeypatch.delenv("MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS", raising=False)
    result = lambda_svc._mock_no_code_response(_cfn_event(url))
    assert result["body"]["statusCode"] == 200
    assert received == []


def test_no_code_lambda_posts_success_when_enabled(response_url_server, monkeypatch):
    from ministack.services import lambda_svc
    url, received = response_url_server
    monkeypatch.setenv("MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS", "1")
    monkeypatch.setenv("MINISTACK_CUSTOM_RESOURCE_MOCK_DATA", _MOCK_DATA)
    lambda_svc._mock_no_code_response(_cfn_event(url))
    assert len(received) == 1
    assert received[0]["Status"] == "SUCCESS"
    assert received[0]["Data"]["Name"] == "n"
    assert received[0]["Data"]["SecretKey"] == "mock-secret"
    assert "ServiceToken" not in received[0]["Data"]


def test_mock_data_defaults_to_empty(response_url_server, monkeypatch):
    from ministack.services import lambda_svc
    url, received = response_url_server
    monkeypatch.setenv("MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS", "1")
    monkeypatch.delenv("MINISTACK_CUSTOM_RESOURCE_MOCK_DATA", raising=False)
    lambda_svc._mock_no_code_response(_cfn_event(url))
    assert received[0]["Data"] == {"Name": "n"}


@pytest.mark.parametrize("raw", ["not json", "[1, 2]", ""])
def test_mock_data_ignores_invalid_values(raw, monkeypatch):
    from ministack.services import lambda_svc
    from ministack.services.cloudformation import custom_resource
    monkeypatch.setenv("MINISTACK_CUSTOM_RESOURCE_MOCK_DATA", raw)
    assert lambda_svc._custom_resource_mock_data() == {}
    assert custom_resource._mock_data() == {}


@pytest.mark.parametrize("enabled", [False, True])
def test_errored_invoke_with_response_url(enabled, response_url_server, monkeypatch):
    from ministack.services import lambda_svc
    url, received = response_url_server
    if enabled:
        monkeypatch.setenv("MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS", "1")
    else:
        monkeypatch.delenv("MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS", raising=False)
    monkeypatch.setattr(lambda_svc, "LAMBDA_STRICT", False)
    monkeypatch.setattr(lambda_svc, "LAMBDA_EXECUTOR", "local")
    failed = {"error": True, "body": "boom"}
    monkeypatch.setattr(lambda_svc, "_execute_function_warm", lambda func, event: failed)
    func = {"config": {"FunctionName": "f", "Runtime": "python3.12"}}
    result = lambda_svc._execute_function(func, _cfn_event(url))
    if enabled:
        assert len(received) == 1 and received[0]["Status"] == "SUCCESS"
        assert result["body"]["statusCode"] == 200
    else:
        assert received == []
        assert result is failed


@pytest.mark.parametrize("enabled", [False, True])
def test_failed_sdk_response_rewritten_only_when_enabled(enabled, monkeypatch):
    from ministack.services.cloudformation import custom_resource
    if enabled:
        monkeypatch.setenv("MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS", "1")
        monkeypatch.setenv("MINISTACK_CUSTOM_RESOURCE_MOCK_DATA", _MOCK_DATA)
    else:
        monkeypatch.delenv("MINISTACK_CUSTOM_RESOURCE_SYNTHETIC_SUCCESS", raising=False)
    custom_resource.reset()
    custom_resource.register_token("tok")
    assert custom_resource.deliver_response(
        "tok", {"Status": "FAILED", "Reason": "Cannot find module 'aws-sdk'"}
    )
    result = custom_resource._pending["tok"]["result"]
    custom_resource.reset()
    if enabled:
        assert result["Status"] == "SUCCESS"
        assert result["Data"] == json.loads(_MOCK_DATA)
    else:
        assert result["Status"] == "FAILED"


# ---------------------------------------------------------------------------
# MINISTACK_CFN_NOOP_CUSTOM_RESOURCES
# ---------------------------------------------------------------------------

def test_noop_custom_resources_off_by_default(monkeypatch):
    from ministack.services.cloudformation import provisioners
    monkeypatch.delenv("MINISTACK_CFN_NOOP_CUSTOM_RESOURCES", raising=False)
    assert provisioners._resource_handler("Custom::LogRetention") is None
    assert provisioners._resource_handler("Custom::S3AutoDeleteObjects") is None


def test_noop_custom_resources_listed_types_only(monkeypatch):
    from ministack.services.cloudformation import provisioners
    monkeypatch.setenv(
        "MINISTACK_CFN_NOOP_CUSTOM_RESOURCES", "Custom::LogRetention, Custom::S3AutoDeleteObjects"
    )
    pid, attrs = provisioners._provision_resource("Custom::LogRetention", "LR", {}, "stk")
    assert "stub" in pid and attrs == {}
    pid, _ = provisioners._update_resource("Custom::S3AutoDeleteObjects", pid, {}, {}, "stk", "AD")
    assert "stub" in pid
    provisioners._delete_resource("Custom::LogRetention", pid, {}, "stk", "LR")
    assert provisioners._resource_handler("Custom::Other") is None


# ---------------------------------------------------------------------------
# MINISTACK_CODEPIPELINE_CONTINUE_ON_ERROR / MINISTACK_CODEPIPELINE_SELF_MUTATE
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [(None, False), ("0", False), ("1", True)])
def test_codepipeline_continue_on_error(value, expected, monkeypatch):
    import ministack.services.codepipeline_runtime as rt
    if value is None:
        monkeypatch.delenv("MINISTACK_CODEPIPELINE_CONTINUE_ON_ERROR", raising=False)
    else:
        monkeypatch.setenv("MINISTACK_CODEPIPELINE_CONTINUE_ON_ERROR", value)
    assert rt._continue_on_error() is expected


@pytest.mark.parametrize("value,expected", [(None, False), ("0", False), ("1", True)])
def test_codepipeline_self_mutate_action(value, expected, monkeypatch):
    import ministack.services.codepipeline_runtime as rt
    if value is None:
        monkeypatch.delenv("MINISTACK_CODEPIPELINE_SELF_MUTATE", raising=False)
    else:
        monkeypatch.setenv("MINISTACK_CODEPIPELINE_SELF_MUTATE", value)
    monkeypatch.delenv("MINISTACK_CODEPIPELINE_REAL_BUILD", raising=False)
    calls = []

    async def fake_self_mutate(pipeline_def):
        calls.append(pipeline_def)
        return ["stack"]

    monkeypatch.setattr(rt, "_self_mutate", fake_self_mutate)
    action = {"name": "SelfMutate", "configuration": {"ProjectName": "p"}}
    asyncio.run(rt._action_build_codebuild(action, {}, {"name": "pipe"}))
    assert bool(calls) is expected


# ---------------------------------------------------------------------------
# CodeStar Connections: no default ProviderType
# ---------------------------------------------------------------------------

def test_csc_create_without_provider_fails(codestar_connections):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        codestar_connections.create_connection(ConnectionName="no-provider-conn")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_csc_create_provider_taken_from_host(codestar_connections):
    host = codestar_connections.create_host(
        Name="h", ProviderType="GitHubEnterpriseServer", ProviderEndpoint="https://ghe.example.com"
    )
    arn = codestar_connections.create_connection(
        ConnectionName="host-conn", HostArn=host["HostArn"]
    )["ConnectionArn"]
    got = codestar_connections.get_connection(ConnectionArn=arn)["Connection"]
    assert got["ProviderType"] == "GitHubEnterpriseServer"


def test_cfn_connection_without_provider_raises():
    from ministack.services.cloudformation import provisioners
    with pytest.raises(ValueError):
        provisioners._codestar_connection_create("Conn", {"ConnectionName": "c"}, "stk")
