import json, os, time
from pathlib import Path
import grpc
from google.protobuf import descriptor_pool, message_factory

import acyclic_sdk.generated.filesystem.v2.filesystem_pb2 as filesystem_pb2
import acyclic_sdk.generated.harness.v2.harness_pb2 as harness_pb2


def fill(message, context):
    if message.DESCRIPTOR.full_name == "acyclic.protocol.v1.ProtocolIdentity":
        message.version = "2"
        message.descriptor_digest = "8efc8c682b2ba1025b1221dd203685bdf999d04e87568fad3acdf0e428bd84cf"
        return
    if message.DESCRIPTOR.full_name == "acyclic.harness.v2.Authority":
        message.kind = 5
        message.id = "00000000-0000-0000-0000-000000000001"
        return
    if message.DESCRIPTOR.full_name == "acyclic.harness.v2.Scope":
        message.id = "scope:fixture"
        message.capabilities.extend(["operation:observe", "operation:cancel"])
        message.issuer = "fixture"
        message.proof = b"\x01" * 32
        return
    for field in message.DESCRIPTOR.fields:
        if field.message_type is not None:
            if field.is_repeated:
                child = getattr(message, field.name).add()
                fill(child, context)
            else:
                fill(getattr(message, field.name), context)
            continue
        if field.is_repeated:
            continue
        if field.type == field.TYPE_STRING:
            if field.name == "operation_id":
                setattr(message, field.name, "11111111-1111-1111-1111-111111111111")
                continue
            values = {
                "name": "fixture",
                "workspace_id": "fixture-workspace",
                "operation_id": "fixture-op",
                "path": "/hello",
                "source": "/hello",
                "destination_name": "fork",
                "protocol": "current",
                "action_type": "fixture",
                "canonical_action_json": "{}",
            }
            setattr(message, field.name, values.get(field.name, f"fixture-{context}"))
        elif field.type == field.TYPE_BYTES:
            values = {"contents": b"rust-fixture", "canonical_action_json": b"{}"}
            setattr(message, field.name, values.get(field.name, b"fixture"))
        elif field.type in (field.TYPE_UINT32, field.TYPE_UINT64, field.TYPE_INT32, field.TYPE_INT64):
            setattr(message, field.name, 1)
        elif field.type == field.TYPE_BOOL:
            setattr(message, field.name, False)


def invoke(channel, service, method):
    request_cls = message_factory.GetMessageClass(method.input_type)
    output_cls = message_factory.GetMessageClass(method.output_type)
    request = request_cls()
    fill(request, method.name)
    path = f"/{service.full_name}/{method.name}"
    if method.client_streaming:
        call = channel.stream_unary(path, request_serializer=request.SerializeToString, response_deserializer=output_cls.FromString)
        return [call(iter([request]), timeout=5)], 1
    if method.server_streaming:
        call = channel.unary_stream(path, request_serializer=request.SerializeToString, response_deserializer=output_cls.FromString)
        return list(call(request, timeout=5)), 1
    call = channel.unary_unary(path, request_serializer=request.SerializeToString, response_deserializer=output_cls.FromString)
    return [call(request, timeout=5)], 1


def has_field(value, name):
    try:
        return value.HasField(name)
    except (AttributeError, ValueError):
        return False


def decoded_summary(value, depth=0):
    if depth > 3:
        return {"type": value.DESCRIPTOR.full_name, "truncated": True}
    result = {"type": value.DESCRIPTOR.full_name, "fields": {}}
    for field, item in value.ListFields():
        if field.message_type is not None:
            if field.is_repeated:
                result["fields"][field.name] = [decoded_summary(child, depth + 1) for child in item[:8]]
            else:
                result["fields"][field.name] = decoded_summary(item, depth + 1)
        elif field.is_repeated:
            result["fields"][field.name] = list(item[:8])
        elif field.type == field.TYPE_BYTES:
            result["fields"][field.name] = {"byte_length": len(item), "sha256": __import__("hashlib").sha256(item).hexdigest()}
        elif field.enum_type is not None:
            result["fields"][field.name] = {"number": int(item), "name": field.enum_type.values_by_number[int(item)].name}
        else:
            result["fields"][field.name] = item
    return result


def generic_semantic(value, known_output):
    summary = decoded_summary(value)
    fields = summary["fields"]
    if not fields:
        return f"mismatch: expected decoded {known_output}; response contained no present fields"
    identity_names = {"id", "operation_id", "workspace_id", "generation_id", "object_id", "cursor", "path", "name", "version"}
    for name, item in fields.items():
        if name in identity_names and isinstance(item, str) and not item:
            return f"mismatch: decoded identity {name} is empty"
        if isinstance(item, dict) and "name" in item and item["name"].endswith("UNSPECIFIED"):
            return f"mismatch: decoded enum {name} is unspecified"
    return f"passed: decoded fields for {known_output}"

def actual(method_name, responses):
    if not responses:
        return {"response": "empty"}
    value = responses[0]
    if method_name == "Handshake":
        if not has_field(value, "protocol"):
            return decoded_summary(value)
        return {"protocol_version": value.protocol.version, "descriptor_digest": value.protocol.descriptor_digest, "supported_present": has_field(value, "supported")}
    if method_name == "Submit":
        return {"state": value.state, "operation_id": value.operation.operation_id if value.HasField("operation") else ""}
    if method_name == "Replay":
        return {"live": value.live, "generation": value.generation, "from_revision": value.from_revision, "through_revision": value.through_revision, "event_count": len(value.events)}
    if method_name == "Observe":
        if not has_field(value, "operation") and not has_field(value, "owner"):
            return decoded_summary(value)
        return {"state": value.state, "operation_id": value.operation.operation_id if has_field(value, "operation") else "", "owner_id": value.owner.id if has_field(value, "owner") else "", "revision": value.revision}
    if method_name == "Cancel":
        try:
            status = value.status
        except (AttributeError, ValueError):
            status = None
        return {"status_present": status is not None, "state": status.state if status is not None else None, "revision": status.revision if status is not None else None, "operation_id": value.operation.operation_id if value.HasField("operation") else ""}
    return decoded_summary(value)

def semantic(method_name, responses, known_output):
    if method_name == "Export":
        if len(responses) != 1:
            return "mismatch: expected one export chunk"
        chunk = responses[0]
        if (chunk.cursor, chunk.object_id, chunk.contents, chunk.terminal) != (b"fixture-export-cursor-1", b"fixture-export-object-1", b"rust-owned-filesystem-export", True):
            return "mismatch: export chunk differs from Rust fixture"
        return "passed: rust export chunk"
    if method_name == "Handshake":
        if len(responses) != 1 or not has_field(responses[0], "protocol") or responses[0].protocol.version != "2":
            return "mismatch: protocol identity"
        return "passed: protocol+capabilities"
    if method_name == "Submit":
        admission = responses[0] if len(responses) == 1 else None
        if admission is None or admission.state != 1 or not admission.HasField("operation"):
            return "mismatch: expected accepted admission"
        return "passed: accepted"
    if method_name == "Replay":
        delivery = responses[0] if len(responses) == 1 else None
        if delivery is None or delivery.live or not delivery.events:
            return "mismatch: expected one non-live delivery"
        return "passed: live_delivery_false"
    if method_name == "Observe":
        status = responses[0] if len(responses) == 1 else None
        if status is None or status.state != 1 or not has_field(status, "owner") or status.revision != 0:
            return "mismatch: expected running status"
        return "passed: running_status"
    if method_name == "Cancel":
        response = responses[0] if len(responses) == 1 else None
        try:
            status = response.status if response is not None else None
        except (AttributeError, ValueError):
            status = None
        if status is None or status.state != 4 or status.revision != 1:
            return "mismatch: expected cancelled revision 1"
        return "passed: cancelled_revision"
    if responses:
        return generic_semantic(responses[0], known_output)
    return f"mismatch: expected {known_output}; empty response"


def main():
    graph = json.loads(Path(os.environ["SEED_GRAPH"]).read_text(encoding="utf-8"))
    address = os.environ.get("FIXTURE_GRPC_ADDRESS", "127.0.0.1:58315")
    output = Path(os.environ["TRACE_OUTPUT"])
    channel = grpc.insecure_channel(address)
    rows = []
    try:
        for scenario in graph["steps"]:
            family = scenario["family"]
            module = filesystem_pb2 if family == "filesystem" else harness_pb2
            service = next(iter(module.DESCRIPTOR.services_by_name.values()))
            method = service.methods_by_name[scenario["operation"].title().replace("_", "")]
            started = time.time()
            row = {"order": scenario["order"], "family": family, "operation": scenario["operation"], "seed": scenario["seed"], "depends_on": scenario["depends_on"], "known_output": scenario["known_output"], "execution_mode": "remote", "transport": "grpc"}
            try:
                responses, frames = invoke(channel, service, method)
                row.update(status="passed", request_frame_count=frames, response_count=len(responses), actual=actual(method.name, responses), semantic_check=semantic(method.name, responses, scenario["known_output"]))
            except Exception as error:
                row.update(status="failed", request_frame_count=1, response_count=0, error=repr(error), semantic_check="transport_error")
            row["elapsed_ms"] = round((time.time() - started) * 1000, 3)
            rows.append(row)
    finally:
        channel.close()
    document = {"schema": "acyclic.sdk.python.rust-seed35-trace.v1", "consumer": "python", "address": address, "execution_mode": "remote", "steps": rows, "passed": sum(x["status"] == "passed" for x in rows), "failed": sum(x["status"] == "failed" for x in rows), "semantic_mismatches": sum("mismatch:" in x.get("semantic_check", "") for x in rows)}
    output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"passed": document["passed"], "failed": document["failed"], "semantic_mismatches": document["semantic_mismatches"]}))


if __name__ == "__main__":
    main()








