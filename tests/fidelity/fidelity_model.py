"""Check what MiniStack puts on the wire against the botocore service model of the operation.

boto3 parses a response into whatever the model allows, so a test that reads only the parsed result
cannot see a member the model does not define or a value of the wrong type. ``WireRecorder`` keeps the
raw body of every call and ``problems()`` compares the last one with the operation's output shape, or
an error code with the operation's modelled errors plus the service's common errors. JSON protocols only.
"""
import json

# https://docs.aws.amazon.com/secretsmanager/latest/apireference/CommonErrors.html
COMMON_ERRORS = {
    "secretsmanager": {
        "AccessDeniedException", "DecryptionFailure", "EncryptionFailure", "IncompleteSignature",
        "InternalFailure", "InternalServiceError", "InvalidAction", "InvalidClientTokenId",
        "InvalidNextTokenException", "InvalidParameterException", "InvalidRequestException",
        "LimitExceededException", "MalformedPolicyDocumentException", "NotAuthorized", "OptInRequired",
        "PreconditionNotMetException", "PublicPolicyException", "RequestExpired", "ResourceExistsException",
        "ResourceNotFoundException", "ServiceUnavailable", "ThrottlingException", "ValidationError",
        "ValidationException",
    },
}


def shape_problems(shape, value, path="$"):
    """Where *value*, decoded from the wire, does not fit the botocore *shape*."""
    kind = shape.type_name
    if kind == "structure":
        if not isinstance(value, dict):
            return [f"{path}: expected an object, got {type(value).__name__}"]
        problems = [f"{path}.{name}: required member missing" for name in shape.required_members
                    if name not in value]
        for name, item in value.items():
            if name not in shape.members:
                problems.append(f"{path}.{name}: not a member of {shape.name}")
            else:
                problems += shape_problems(shape.members[name], item, f"{path}.{name}")
        return problems
    if kind == "list":
        if not isinstance(value, list):
            return [f"{path}: expected a list, got {type(value).__name__}"]
        return [p for i, item in enumerate(value) for p in shape_problems(shape.member, item, f"{path}[{i}]")]
    if kind == "map":
        if not isinstance(value, dict):
            return [f"{path}: expected a map, got {type(value).__name__}"]
        return [p for k, item in value.items() for p in shape_problems(shape.value, item, f"{path}[{k!r}]")]
    expected = {
        "string": (str,), "blob": (str,), "boolean": (bool,), "integer": (int,), "long": (int,),
        "float": (int, float), "double": (int, float), "timestamp": (int, float, str),
    }.get(kind)
    if expected is None:
        return [f"{path}: shape type {kind} is not checked"]
    if isinstance(value, bool) and bool not in expected:
        return [f"{path}: expected {kind}, got bool"]
    if not isinstance(value, expected):
        return [f"{path}: expected {kind}, got {type(value).__name__}"]
    return []


class WireRecorder:
    """Records the raw responses of one boto3 client."""

    def __init__(self, client):
        self.service = client.meta.service_model.service_name
        self.calls = []
        client.meta.events.register("after-call", self._record)

    def _record(self, http_response, model, **_):
        self.calls.append((model, http_response.status_code, http_response.content))

    def problems(self):
        """How the last response differs from the model; empty when it fits."""
        model, status, body = self.calls[-1]
        decoded = json.loads(body) if body else {}
        if status < 300:
            return shape_problems(model.output_shape, decoded) if model.output_shape else []
        code = str(decoded.get("__type", "")).rsplit("#", 1)[-1]
        allowed = {shape.name for shape in model.error_shapes} | COMMON_ERRORS.get(self.service, set())
        return [] if code in allowed else [f"error {code!r} is not modelled for {model.name}"]
