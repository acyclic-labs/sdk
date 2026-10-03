from __future__ import annotations

import asyncio

import grpc

from acyclic_sdk.generated.actors.v1 import actors_pb2, actors_pb2_grpc
from acyclic_sdk.generated.stream.v2 import stream_pb2, stream_pb2_grpc


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
