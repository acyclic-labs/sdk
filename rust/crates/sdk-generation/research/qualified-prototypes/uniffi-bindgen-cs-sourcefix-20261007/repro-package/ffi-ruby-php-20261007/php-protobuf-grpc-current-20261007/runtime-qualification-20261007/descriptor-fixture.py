import json
import os
import sys
import threading
import time
from concurrent import futures
from pathlib import Path

import grpc
from google.protobuf import descriptor_pool, message_factory

sys.path.insert(0, "/tmp/actors-php-fixture/python")
from actors.v1 import actors_pb2_grpc

EVENTS = Path("/tmp/actors-php-fixture/events.jsonl")
TOKEN = "php-fixture-token"
U64_VALUE = int(os.environ.get("ACTORS_PHP_U64", "18446744073709551615"))

def event(kind, **details):
    EVENTS.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"kind": kind, **details}) + "\n")

def authorized(context):
    metadata = dict((item.key, item.value) for item in context.invocation_metadata())
    return metadata.get("authorization") == "Bearer " + TOKEN

def set_scalar_fields(message, method_name):
    """Populate response scalars from the generated descriptor, never a second schema."""
    for field in message.DESCRIPTOR.fields:
        if field.label == field.LABEL_REPEATED or field.type == field.TYPE_MESSAGE:
            continue
        if field.type == field.TYPE_UINT64 or field.type == field.TYPE_FIXED64:
            setattr(message, field.name, U64_VALUE)
        elif field.type == field.TYPE_STRING:
            setattr(message, field.name, "php-fixture")
        elif field.type == field.TYPE_BYTES:
            setattr(message, field.name, b"php-fixture")
        elif field.type == field.TYPE_BOOL:
            setattr(message, field.name, True)
        elif field.type in (field.TYPE_UINT32, field.TYPE_FIXED32, field.TYPE_INT32, field.TYPE_SINT32):
            setattr(message, field.name, 7)

def request_value(request, name):
    field = request.DESCRIPTOR.fields_by_name.get(name)
    return getattr(request, name) if field is not None else None

class FixtureServicer(actors_pb2_grpc.ActorsServiceServicer):
    pass

def make_handler(method_descriptor):
    method_name = method_descriptor.name
    response_cls = message_factory.GetMessageClass(method_descriptor.output_type)
    def handler(self, request, context):
        event("received", method=method_name)
        if not authorized(context):
            event("unauthenticated", method=method_name)
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "fixture authorization required")
        actor_id = request_value(request, "actor_id")
        if actor_id == "error":
            event("server_abort", method=method_name)
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "fixture denied")
        if method_name == "InvokeActor" and request_value(request, "method") == "sleep":
            event("started", method=method_name)
            for _ in range(200):
                if not context.is_active():
                    event("cancelled", method=method_name)
                    return response_cls()
                time.sleep(0.05)
            event("completed", method=method_name)
        response = response_cls()
        set_scalar_fields(response, method_name)
        # Fill message fields using the same descriptor-driven response shape.
        for field in response.DESCRIPTOR.fields:
            if field.type == field.TYPE_MESSAGE and field.label != field.LABEL_REPEATED:
                nested = getattr(response, field.name)
                set_scalar_fields(nested, method_name)
                if hasattr(nested, "actor_id"):
                    nested.actor_id = str(actor_id or "php-fixture")
        event("sent", method=method_name)
        return response
    return handler

for method in descriptor_pool.Default().FindServiceByName("acyclic.actors.v1.ActorsService").methods:
    setattr(FixtureServicer, method.name, make_handler(method))

server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
actors_pb2_grpc.add_ActorsServiceServicer_to_server(FixtureServicer(), server)
port = server.add_insecure_port("127.0.0.1:0")
server.start()
Path("/tmp/actors-php-fixture/endpoint").write_text(f"127.0.0.1:{port}\n", encoding="utf-8")
event("started_server", endpoint=f"127.0.0.1:{port}", pid=os.getpid())
try:
    server.wait_for_termination()
finally:
    event("stopped_server")
