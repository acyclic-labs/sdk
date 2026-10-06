from __future__ import annotations

import asyncio
import io
import urllib.error

import grpc
from google.protobuf import json_format
import pytest

from acyclic_sdk.generated.actors.v1 import actors_pb2, actors_pb2_grpc
from acyclic_sdk.generated.filesystem.v2 import filesystem_pb2
from acyclic_sdk.generated.objects.v2 import objects_pb2
from acyclic_sdk.generated.protocol.v1 import protocol_pb2
from acyclic_sdk.generated.stream.v2 import stream_pb2, stream_pb2_grpc
from acyclic_sdk.generated.workers.v1 import workers_pb2
from acyclic_sdk.remote import (
    Client,
    HANDSHAKE,
    HTTP_ROUTES,
    MessageUnknown,
    RustActorsActorsCreateActorRequest,
    RustFilesystemFilesystemGetSourceStateResponse,
    RustHttpError,
    RustObjectsObjectsPutObjectRequest,
    RustObjectsObjectsGetObjectResponse,
    RustStreamStreamFollowRequest,
    RustWorkersWorkersInvokeDeploymentRequest,
    RustWorkersWorkersInvokeVersionRequest,
    RustWorkersWorkersSelectDeploymentRequest,
)


def test_wire_types_preserve_large_values_bytes_oneof_and_presence() -> None:
    large = 2**63 + 17
    request = actors_pb2.CreateActorRequest(
        code_sha256=b"\x00\xff" * 16,
        home_region="eu-west",
        limits=actors_pb2.ActorLimits(memory_bytes=large),
        subscriptions=[
            actors_pb2.SubscriptionSpec(
                subscription_id="events",
                start=actors_pb2.SubscriptionStart(cursor=large),
            )
        ],
    )

    assert request.code_sha256 == b"\x00\xff" * 16
    assert request.limits.memory_bytes == large
    assert request.subscriptions[0].start.WhichOneof("start") == "cursor"
    assert request.subscriptions[0].start.cursor == large
    assert request.subscriptions[0].placement_anchor is False

    observation = actors_pb2.SubscriptionObservation(failed_cursor=large)
    assert observation.HasField("failed_cursor")
    observation.ClearField("failed_cursor")
    assert not observation.HasField("failed_cursor")

    outcome = stream_pb2.AppendResponse(
        committed=stream_pb2.AppendReceipt(start=large, end=large + 1)
    )
    assert outcome.WhichOneof("outcome") == "committed"
    assert outcome.committed.start == large


def test_unknown_wire_fields_remain_message_level_opaque_data() -> None:
    message = objects_pb2.GetObjectResponse.FromString(b"\x28\x01")
    decoded = RustObjectsObjectsGetObjectResponse.from_wire(message)
    assert decoded.frame is None
    assert isinstance(decoded.unknown, MessageUnknown)
    assert decoded.unknown.raw == message.SerializeToString()


def test_python_dtos_preserve_oneof_exclusivity_presence_and_unknown_enum_values() -> None:
    with pytest.raises(ValueError, match="at most one arm"):
        RustObjectsObjectsPutObjectRequest(body=b"body", complete=True)

    present = RustWorkersWorkersSelectDeploymentRequest.from_wire(
        workers_pb2.SelectDeploymentRequest(
            alias="canary",
            version_sha256=b"\xab" * 32,
            expected_revision=0,
            idempotency_key="request-1",
        )
    )
    absent = RustWorkersWorkersSelectDeploymentRequest.from_wire(
        workers_pb2.SelectDeploymentRequest(
            alias="canary", version_sha256=b"\xab" * 32, idempotency_key="request-1"
        )
    )
    assert present.expected_revision == 0
    assert absent.expected_revision is None

    response = RustFilesystemFilesystemGetSourceStateResponse.from_wire(
        filesystem_pb2.SourceResponse(state=99)
    )
    assert response.state.value == 99
    assert response.state.is_unknown


def test_client_streaming_facade_accepts_async_request_iterators() -> None:
    async def run() -> None:
        client = object.__new__(Client)

        async def ensure_transport(*_args: object, **_kwargs: object) -> str:
            return "grpc"

        class ObjectsStub:
            async def PutObject(self, request_iterator, timeout=None):  # noqa: N802
                values = [item async for item in request_iterator]
                assert len(values) == 2
                assert timeout == 1.25
                return objects_pb2.ObjectInfo(etag="streamed")

        client._ensure_transport = ensure_transport
        client._objects = ObjectsStub()
        request = RustObjectsObjectsPutObjectRequest.from_wire(objects_pb2.PutObjectRequest())

        async def requests():
            yield request
            yield request

        result = await client.objects_Objects_PutObject(requests(), timeout=1.25)
        assert result.etag == "streamed"

    asyncio.run(run())


class _Actors(actors_pb2_grpc.ActorsServiceServicer):
    def __init__(self) -> None:
        self.metadata: tuple[tuple[str, str], ...] = ()

    async def CreateActor(self, request, context):  # noqa: N802
        self.metadata = tuple((key, value) for key, value in context.invocation_metadata())
        return actors_pb2.CreateActorResponse(
            actor=actors_pb2.ActorObservation(
                actor_id="actor-1",
                code_sha256=request.code_sha256,
                home_region=request.home_region,
                checkpoint_epoch=request.limits.memory_bytes,
            )
        )


class _Stream(stream_pb2_grpc.StreamServiceServicer):
    def __init__(self) -> None:
        self.cancelled = asyncio.Event()

    async def Follow(self, request, context):  # noqa: N802
        yield stream_pb2.ReadResponse(
            record=stream_pb2.Record(sequence=getattr(request, "from"), value=b"first")
        )
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


async def _start_server(servicer, add_servicer) -> tuple[grpc.aio.Server, int]:
    server = grpc.aio.server()
    add_servicer(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


def test_real_loopback_unary_call_carries_bearer_metadata() -> None:
    async def run() -> None:
        servicer = _Actors()
        server, port = await _start_server(
            servicer, actors_pb2_grpc.add_ActorsServiceServicer_to_server
        )
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        try:
            stub = actors_pb2_grpc.ActorsServiceStub(channel)
            result = await stub.CreateActor(
                actors_pb2.CreateActorRequest(
                    code_sha256=b"code",
                    home_region="local",
                    limits=actors_pb2.ActorLimits(memory_bytes=2**63 + 1),
                ),
                metadata=(("authorization", "Bearer test-token"),),
            )
            assert result.actor.actor_id == "actor-1"
            assert result.actor.checkpoint_epoch == 2**63 + 1
            assert ("authorization", "Bearer test-token") in servicer.metadata
        finally:
            await asyncio.wait_for(server.stop(0), timeout=2)
            await asyncio.wait_for(channel.close(), timeout=2)

    asyncio.run(run())


def test_real_loopback_server_stream_can_be_cancelled() -> None:
    async def run() -> None:
        servicer = _Stream()
        server, port = await _start_server(
            servicer, stream_pb2_grpc.add_StreamServiceServicer_to_server
        )
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        try:
            stub = stream_pb2_grpc.StreamServiceStub(channel)
            request = stream_pb2.FollowRequest(path="events")
            setattr(request, "from", 2**63 + 3)
            call = stub.Follow(request, metadata=(("authorization", "Bearer stream-token"),))
            first = await call.read()
            assert first.record.sequence == 2**63 + 3
            assert first.record.value == b"first"
            assert call.cancel()
            assert await call.code() == grpc.StatusCode.CANCELLED
            await asyncio.wait_for(servicer.cancelled.wait(), timeout=2)
        finally:
            await asyncio.wait_for(server.stop(0), timeout=2)
            await asyncio.wait_for(channel.close(), timeout=2)

    asyncio.run(run())


class _HttpFixtureResponse:
    def __init__(
        self,
        body: bytes = b"",
        *,
        status: int = 200,
        url: str = "http://fixture.test",
        content_type: str = "application/json",
    ) -> None:
        self._body = io.BytesIO(body)
        self.status = status
        self.url = url
        self.headers = {"content-type": content_type}
        self.closed = False

    def geturl(self) -> str:
        return self.url

    def getcode(self) -> int:
        return self.status

    def read(self, limit: int = -1) -> bytes:
        return self._body.read(limit)

    def readline(self, limit: int = -1) -> bytes:
        return self._body.readline(limit)

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> "_HttpFixtureResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _fixture_client() -> Client:
    client = object.__new__(Client)
    client._http_base = "http://fixture.test"
    client.credentials = type("Credentials", (), {"bearer_token": "fixture-token"})()
    client._transports = {}
    return client


def _handshake_json(family: str) -> bytes:
    identity = HANDSHAKE[family]
    response = protocol_pb2.HandshakeResponse(
        protocol=protocol_pb2.ProtocolIdentity(
            version=identity["version"],
            descriptor_digest=identity["descriptor_digest"],
        ),
        supported=protocol_pb2.CapabilitySet(
            capabilities=[
                protocol_pb2.Capability(name=family, version=identity["version"]),
            ]
        ),
    )
    return json_format.MessageToJson(response).encode("utf-8")


def test_http_handshake_fixture_is_bodyless_get_with_auth_and_contract_headers() -> None:
    client = _fixture_client()
    calls: list[object] = []
    timeouts: list[object] = []
    response = _HttpFixtureResponse(
        _handshake_json("actors"),
        url=client._http_base + "/v1/sdk/actors/handshake",
    )

    def open_http(request: object, *_args: object, **_kwargs: object) -> _HttpFixtureResponse:
        calls.append(request)
        timeouts.append(_kwargs.get("timeout"))
        return response

    client._open_http = open_http
    client._http_handshake("actors", timeout=1.25)

    request = calls[0]
    assert request.get_method() == "GET"
    assert request.data is None
    assert request.get_header("Authorization") == "Bearer fixture-token"
    assert request.get_header("Accept") == "application/json"
    assert timeouts == [1.25]
    assert response.closed


def test_http_request_fixture_preserves_route_method_body_and_typed_error() -> None:
    client = _fixture_client()
    route = next(iter(HTTP_ROUTES["actors"].values()))
    request = actors_pb2.CreateActorRequest(home_region="fixture")
    response = _HttpFixtureResponse(b"{}")
    calls: list[object] = []

    def open_http(http_request: object, *_args: object, **_kwargs: object) -> _HttpFixtureResponse:
        calls.append(http_request)
        return response

    client._open_http = open_http
    client._http_request("actors", route["method"], route["path"], request)
    http_request = calls[0]
    assert http_request.get_method() == route["method"]
    assert http_request.data == json_format.MessageToJson(request).encode("utf-8")
    assert http_request.get_header("Content-type") == "application/json"
    assert http_request.get_header("Authorization") == "Bearer fixture-token"

    error_body = b'{"code":"conflict"}'
    error = urllib.error.HTTPError(
        client._http_base + route["path"],
        409,
        "conflict",
        {"content-type": "application/json"},
        io.BytesIO(error_body),
    )
    client._open_http = lambda _request, **_kwargs: (_ for _ in ()).throw(error)
    with pytest.raises(RustHttpError) as raised:
        client._http_request("actors", route["method"], route["path"], request)
    assert raised.value.status == 409
    assert raised.value.detail == error_body


def test_worker_http_route_templates_encode_binary_and_alias_segments() -> None:
    client = _fixture_client()
    calls: list[object] = []

    def open_http(request: object, *_args: object, **_kwargs: object) -> _HttpFixtureResponse:
        calls.append(request)
        return _HttpFixtureResponse(
            json_format.MessageToJson(
                workers_pb2.InvokeResponse(status=200, body=b"ok", resolved_sha256=b"\xcd" * 32)
            ).encode("utf-8"),
            url=request.full_url,
        )

    client._open_http = open_http

    async def run() -> None:
        async def ensure_transport(*_args: object, **_kwargs: object) -> str:
            return "http_json"

        client._ensure_transport = ensure_transport
        version_request = RustWorkersWorkersInvokeVersionRequest.from_wire(
            workers_pb2.InvokeVersionRequest(
                version_sha256=b"\xab" * 32, method="GET", url="/"
            )
        )
        version = await client.workers_Workers_InvokeVersion(version_request)
        assert version.status == 200
        assert version.body == b"ok"
        alias_request = RustWorkersWorkersInvokeDeploymentRequest.from_wire(
            workers_pb2.InvokeDeploymentRequest(alias="canary/blue ?", method="GET", url="/")
        )
        deployment = await client.workers_Workers_InvokeDeployment(alias_request)
        assert deployment.status == 200

    asyncio.run(run())
    assert [request.get_method() for request in calls] == ["POST", "POST"]
    assert calls[0].full_url.endswith("/v1/workers/versions/" + ("ab" * 32) + "/invoke")
    assert calls[1].full_url.endswith("/v1/workers/deployments/canary%2Fblue%20%3F/invoke")

    version_route = HTTP_ROUTES["workers"][
        "acyclic.workers.v1.WorkersService/InvokeVersion"
    ]
    version_request = workers_pb2.InvokeVersionRequest(version_sha256=b"\xab" * 32)
    assert version_route["path_fields"] == {"sha256hex": "version_sha256"}
    assert Client._route_path(version_route, version_request) == (
        "/v1/workers/versions/" + ("ab" * 32) + "/invoke"
    )
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        Client._route_path(
            version_route,
            workers_pb2.InvokeVersionRequest(version_sha256=b"short"),
        )

    alias_route = HTTP_ROUTES["workers"][
        "acyclic.workers.v1.WorkersService/InvokeDeployment"
    ]
    alias_request = workers_pb2.InvokeDeploymentRequest(alias="canary/blue ?")
    assert alias_route["path_fields"] == {"alias": "alias"}
    assert Client._route_path(alias_route, alias_request).endswith(
        "/deployments/canary%2Fblue%20%3F/invoke"
    )


def test_http_handshake_fixture_rejects_redirect_and_wrong_content_type() -> None:
    client = _fixture_client()
    redirect_response = _HttpFixtureResponse(
        _handshake_json("actors"), url="http://other.test/v1/sdk/actors/handshake"
    )
    client._open_http = lambda _request, **_kwargs: redirect_response
    with pytest.raises(RuntimeError, match="redirected"):
        client._http_handshake("actors")
    assert redirect_response.closed

    invalid_content_response = _HttpFixtureResponse(
        _handshake_json("actors"),
        url=client._http_base + "/v1/sdk/actors/handshake",
        content_type="text/plain",
    )
    client._open_http = lambda _request, **_kwargs: invalid_content_response
    with pytest.raises(RuntimeError, match="content type"):
        client._http_handshake("actors")
    assert invalid_content_response.closed

    invalid_identity_response = _HttpFixtureResponse(
        _handshake_json("actors"),
        url=client._http_base + "/v1/sdk/actors/handshake",
    )
    client._open_http = lambda _request, **_kwargs: invalid_identity_response
    original_identity = HANDSHAKE["actors"]["descriptor_digest"]
    HANDSHAKE["actors"]["descriptor_digest"] = "wrong"
    try:
        with pytest.raises(ValueError, match="identity mismatch"):
            client._http_handshake("actors")
    finally:
        HANDSHAKE["actors"]["descriptor_digest"] = original_identity
    assert invalid_identity_response.closed


def test_http_stream_fixture_closes_response_when_cancelled() -> None:
    client = _fixture_client()
    route_name = "acyclic.stream.v2.StreamService/Follow"
    route = HTTP_ROUTES["stream"][route_name]
    family = next(family for family, routes in HTTP_ROUTES.items() if route_name in routes)
    response = _HttpFixtureResponse(b"{}\n", url=client._http_base + route["path"])
    timeouts: list[object] = []

    def open_http(_request: object, *_args: object, **kwargs: object) -> _HttpFixtureResponse:
        timeouts.append(kwargs.get("timeout"))
        return response

    client._open_http = open_http

    async def run() -> None:
        stream = await client._http_stream(
            family,
            route_name,
            stream_pb2.FollowRequest(path="fixture"),
            stream_pb2.ReadResponse,
            timeout=1.25,
        )
        assert stream.cancel()
        assert stream.cancelled()
        assert response.closed
        assert timeouts == [1.25]

    asyncio.run(run())


def test_http_stream_fixture_closes_once_and_client_close_cancels_active_stream() -> None:
    client = _fixture_client()
    route_name = "acyclic.stream.v2.StreamService/Follow"
    route = HTTP_ROUTES["stream"][route_name]
    response = _HttpFixtureResponse(b"{}\n", url=client._http_base + route["path"])
    client._open_http = lambda _request, **_kwargs: response

    class Channel:
        async def close(self) -> None:
            return None

    client._channel = Channel()

    async def run() -> None:
        async def ensure_transport(*_args: object, **_kwargs: object) -> str:
            return "http_json"

        client._ensure_transport = ensure_transport
        stream = await client.stream_Stream_Follow(
            RustStreamStreamFollowRequest.from_wire(stream_pb2.FollowRequest(path="fixture"))
        )
        await client.close()
        assert response.closed
        assert stream.done()
        assert stream.cancel() is False

    asyncio.run(run())


def test_http_body_read_failure_closes_response_and_http_error_body() -> None:
    client = _fixture_client()
    route = next(iter(HTTP_ROUTES["actors"].values()))

    class ReadFailureResponse(_HttpFixtureResponse):
        def read(self, _limit: int = -1) -> bytes:
            raise OSError("fixture read failure")

    response = ReadFailureResponse()
    client._open_http = lambda _request, **_kwargs: response

    async def run_read_failure() -> None:
        async def ensure_transport(*_args: object, **_kwargs: object) -> str:
            return "http_json"

        client._ensure_transport = ensure_transport
        with pytest.raises(OSError, match="fixture read failure"):
            await client.actors_Actors_CreateActor(
                RustActorsActorsCreateActorRequest.from_wire(actors_pb2.CreateActorRequest())
            )

    asyncio.run(run_read_failure())
    assert response.closed

    class ReadFailureFile(io.BytesIO):
        def read(self, _size: int = -1) -> bytes:
            raise OSError("fixture error-body failure")

    error_file = ReadFailureFile()
    error = urllib.error.HTTPError(
        client._http_base + route["path"],
        503,
        "unavailable",
        {"content-type": "application/json"},
        error_file,
    )
    client._open_http = lambda _request, **_kwargs: (_ for _ in ()).throw(error)
    async def run_unary_failure() -> None:
        async def ensure_transport(*_args: object, **_kwargs: object) -> str:
            return "http_json"

        client._ensure_transport = ensure_transport
        with pytest.raises(RustHttpError) as raised:
            await client.actors_Actors_CreateActor(
                RustActorsActorsCreateActorRequest.from_wire(actors_pb2.CreateActorRequest())
            )
        assert raised.value.status == 503

    asyncio.run(run_unary_failure())
    assert error_file.closed

    stream_name = "acyclic.stream.v2.StreamService/Follow"
    stream_route = HTTP_ROUTES["stream"][stream_name]
    stream_response = ReadFailureResponse(
        status=503,
        url=client._http_base + stream_route["path"],
    )
    client._open_http = lambda _request, **_kwargs: stream_response

    async def run_stream_failure() -> None:
        async def ensure_transport(*_args: object, **_kwargs: object) -> str:
            return "http_json"

        client._ensure_transport = ensure_transport
        with pytest.raises(OSError, match="fixture read failure"):
            await client.stream_Stream_Follow(
                RustStreamStreamFollowRequest.from_wire(
                    stream_pb2.FollowRequest(path="fixture")
                )
            )

    asyncio.run(run_stream_failure())
    assert stream_response.closed


def test_http_client_stream_fixture_sends_bounded_ndjson_and_typed_response() -> None:
    client = _fixture_client()
    rpc, route = next(
        (rpc, item)
        for routes in HTTP_ROUTES.values()
        for rpc, item in routes.items()
        if rpc.endswith("/PutObject")
    )
    response = _HttpFixtureResponse(
        json_format.MessageToJson(objects_pb2.ObjectInfo(etag="streamed")).encode("utf-8")
    )
    calls: list[object] = []
    timeouts: list[object] = []

    def open_http(request: object, *_args: object, **kwargs: object) -> _HttpFixtureResponse:
        calls.append(request)
        timeouts.append(kwargs.get("timeout"))
        return response

    client._open_http = open_http

    async def run() -> None:
        request = RustObjectsObjectsPutObjectRequest.from_wire(objects_pb2.PutObjectRequest())

        async def requests():
            yield request
            yield request

        async def ensure_transport(*_args: object, **_kwargs: object) -> str:
            return "http_json"

        client._ensure_transport = ensure_transport
        result = await client.objects_Objects_PutObject(requests(), timeout=1.25)
        assert result.etag == "streamed"

    asyncio.run(run())
    request = calls[0]
    assert request.get_method() == route["method"]
    assert request.get_header("Content-type") == "application/x-ndjson"
    assert request.data.count(b"\n") == 2
    assert timeouts == [1.25]
    assert response.closed
