"""Run every generated Machines RPC against the Rust-owned mTLS fixture.

The semantic assertions mirror the values in sdk-examples/src/tls_fixture.rs;
they are emitted with each receipt so transport success cannot be mistaken for
wire compatibility.
"""
import hashlib
import json
import os
from pathlib import Path

import grpc

from acyclic_sdk.generated.machines.v1 import machines_pb2 as m
from acyclic_sdk.generated.machines.v1 import machines_pb2_grpc as mg


meta = json.loads(Path(os.environ.get("MACHINES_FIXTURE_JSON", os.environ["FIXTURE_META"])).read_text())
creds = grpc.ssl_channel_credentials(
    root_certificates=meta["caCertificate"].encode(),
    private_key=meta["privateKey"].encode(),
    certificate_chain=meta["certificate"].encode(),
)
target = meta["endpoint"].removeprefix("https://")
out = Path(os.environ["SCENARIO_OUT"])
out.mkdir(parents=True, exist_ok=True)
revision = os.environ["SOURCE_REVISION"]
rows = []


def digest(value):
    return {"byte_length": len(value), "sha256": "sha256:" + hashlib.sha256(value).hexdigest()}


def summary(value, depth=0):
    if depth > 4:
        return {"type": value.DESCRIPTOR.full_name, "truncated": True}
    fields = {}
    for field, item in value.ListFields():
        if field.message_type is not None:
            if field.is_repeated:
                fields[field.name] = [summary(child, depth + 1) for child in item[:16]]
            else:
                fields[field.name] = summary(item, depth + 1)
        elif field.is_repeated:
            fields[field.name] = [digest(child) if field.type == field.TYPE_BYTES else item for child in item[:16]]
        elif field.type == field.TYPE_BYTES:
            fields[field.name] = digest(item)
        elif field.enum_type is not None:
            fields[field.name] = {"number": int(item), "name": field.enum_type.values_by_number[int(item)].name}
        else:
            fields[field.name] = item
    return {"type": value.DESCRIPTOR.full_name, "fields": fields}


def assertion(name, ok, detail):
    return {"name": name, "status": "passed" if ok else "failed", "detail": detail}


def semantics(rpc, response):
    if rpc.endswith("QualifyImage"):
        return [assertion("image-echo", response.image.kind == m.IMAGE_KIND_CUSTOM, "Rust fixture echoes Image"), assertion("capability", list(response.capabilities) == [m.CAPABILITY_LIVE_CHECKPOINT], "Rust fixture capability"), assertion("compatibility", response.compatibility_revision == b"\x08" * 32, "Rust fixture compatibility revision")]
    if rpc.endswith("Create"):
        return [assertion("machine", response.HasField("machine") and response.machine.value == b"\x01" * 16, "Rust fixture machine identity"), assertion("operation", response.HasField("operation") and response.operation.value == b"\x02" * 16, "Rust fixture operation identity"), assertion("contract", response.HasField("contract"), "Rust fixture contract")]
    if rpc.endswith("Checkpoint"):
        return [assertion("checkpoint", response.HasField("checkpoint") and response.checkpoint.value == b"\x03" * 16, "Rust fixture checkpoint identity"), assertion("operation", response.HasField("operation") and response.operation.value == b"\x02" * 16, "Rust fixture operation identity")]
    if rpc.endswith("Fork"):
        return [assertion("children", len(response.children) == 2 and response.children[0].value == b"\x04" * 16 and response.children[1].value == b"\x05" * 16, "Rust fixture fork children")]
    if rpc.endswith("ForkMachine"):
        return [assertion("children", len(response.children) == 2, "Rust fixture fork machine children"), assertion("fidelity", response.fidelity == m.FORK_FIDELITY_MEMORY_AND_DISK, "Rust fixture fork fidelity")]
    if rpc.endswith(("Suspend", "Wake", "DestroyMachine")):
        return [assertion("machine", response.HasField("machine"), "Rust fixture mutation machine"), assertion("operation", response.HasField("operation") and response.operation.value == b"\x02" * 16, "Rust fixture operation identity")]
    if rpc.endswith("SetSuspensionPolicy"):
        return [assertion("machine", response.HasField("machine"), "Rust fixture policy machine"), assertion("operation", response.HasField("operation"), "Rust fixture policy operation"), assertion("policy", response.HasField("policy"), "Rust fixture policy echo")]
    if rpc.endswith("DestroyCheckpoint"):
        return [assertion("checkpoint", response.HasField("checkpoint"), "Rust fixture checkpoint mutation"), assertion("operation", response.HasField("operation"), "Rust fixture operation")]
    if rpc.endswith("Recover"):
        return [assertion("operation", response.HasField("operation"), "Rust fixture recovery operation"), assertion("create", response.HasField("create") and response.create.machine.value == b"\x01" * 16, "Rust fixture recovered create")]
    if rpc.endswith("InspectMachine"):
        return [assertion("machine", response.HasField("machine"), "Rust fixture inspected machine"), assertion("running", response.status == m.MACHINE_STATUS_RUNNING, "Rust fixture running state"), assertion("timestamps", response.created_at_unix_ms == 1 and response.changed_at_unix_ms == 1, "Rust fixture state timestamps")]
    if rpc.endswith("InspectCheckpoint"):
        return [assertion("forkable", response.forkable, "Rust fixture forkable checkpoint"), assertion("source", response.HasField("source") and response.source.value == b"\x01" * 16, "Rust fixture checkpoint source")]
    if rpc.endswith("ListMachines"):
        return [assertion("page", len(response.machines) == 1 and response.machines[0].status == m.MACHINE_STATUS_RUNNING and response.next is None, "Rust fixture machine page")]
    if rpc.endswith("Events"):
        return [assertion("event", len(response.events) == 1 and response.events[0].sequence == 1 and response.events[0].kind == m.EVENT_KIND_STATE and response.events[0].state == m.MACHINE_STATUS_RUNNING, "Rust fixture state event"), assertion("cursor", response.next_sequence == 2, "Rust fixture next sequence")]
    if rpc.endswith("Usage"):
        return [assertion("usage", response.elastic_cpu_ns == 1 and response.private_resident_byte_seconds == 1 and response.durable_private_bytes == 1 and response.egress_bytes == 1, "Rust fixture usage counters"), assertion("receipt", response.receipt == b"\x0a", "Rust fixture receipt")]
    if rpc.endswith("Cancel"):
        return [assertion("cancelled", response.status == m.OPERATION_STATUS_CANCELLED, "Rust fixture cancelled operation")]
    if rpc.endswith("InspectOperation"):
        return [assertion("pending", response.status == m.OPERATION_STATUS_PENDING, "Rust fixture pending operation")]
    return []


def put(rpc, shape, response, checks=None, raw=None, extra=None):
    raw = response.SerializeToString() if raw is None else raw
    item = {
        "schema": "acyclic.sdk.rpc-scenario-result.v1",
        "source_revision": revision,
        "status": "passed" if not checks or all(x["status"] == "passed" for x in checks) else "failed",
        "invoked": True,
        "execution_mode": "remote",
        "exit_code": 0,
        "family": "machines",
        "rpc": rpc,
        "shape": shape,
        "transport": "grpc+mtls",
        "checks": ["invocation", "transport", "serialization", "tls-hostname-verification"],
        "response_sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "response_bytes": len(raw),
        "semantic_checks": checks or [],
        "decoded_summary": summary(response) if hasattr(response, "DESCRIPTOR") else {"stream_items": len(response)},
    }
    if shape == "server":
        item["checks"].append("streaming")
    if extra:
        item.update(extra)
    rows.append(item)
    return response


def invoke(rpc, fn):
    response = fn()
    return put(rpc, "unary", response, semantics(rpc, response))


with grpc.secure_channel(target, creds, options=(("grpc.ssl_target_name_override", "localhost"),)) as channel:
    client = mg.MachinesServiceStub(channel)
    machine = m.MachineId(value=bytes.fromhex(meta["machineId"]))
    operation = m.OperationId(value=bytes.fromhex(meta["operationId"]))
    image = m.Image(kind=m.IMAGE_KIND_CUSTOM, custom_digest=b"\0" * 32)
    invoke("acyclic.machines.v1.MachinesService/QualifyImage", lambda: client.QualifyImage(m.QualifyImageRequest(image=image), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Create", lambda: client.Create(m.CreateMachineRequest(image=image), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Checkpoint", lambda: client.Checkpoint(m.CheckpointMachineRequest(machine=machine), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Fork", lambda: client.Fork(m.ForkCheckpointRequest(count=2), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/ForkMachine", lambda: client.ForkMachine(m.ForkMachineRequest(machine=machine, count=2), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Suspend", lambda: client.Suspend(m.MachineMutationRequest(machine=machine), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Wake", lambda: client.Wake(m.MachineMutationRequest(machine=machine), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/SetSuspensionPolicy", lambda: client.SetSuspensionPolicy(m.SetSuspensionPolicyRequest(machine=machine), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/DestroyMachine", lambda: client.DestroyMachine(m.MachineMutationRequest(machine=machine), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/DestroyCheckpoint", lambda: client.DestroyCheckpoint(m.CheckpointMutationRequest(), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Recover", lambda: client.Recover(m.RecoverRequest(), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/InspectMachine", lambda: client.InspectMachine(m.InspectMachineRequest(machine=machine), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/InspectCheckpoint", lambda: client.InspectCheckpoint(m.InspectCheckpointRequest(), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/ListMachines", lambda: client.ListMachines(m.ListMachinesRequest(limit=1), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Events", lambda: client.Events(m.EventsRequest(machine=machine, limit=1), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Usage", lambda: client.Usage(m.UsageRequest(machine=machine), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/Cancel", lambda: client.Cancel(m.OperationRequest(operation=operation), timeout=10))
    invoke("acyclic.machines.v1.MachinesService/InspectOperation", lambda: client.InspectOperation(m.OperationRequest(operation=operation), timeout=10))
    states = list(client.WatchOperation(m.OperationRequest(operation=operation), timeout=10))
    watch_checks = [assertion("stream-count", len(states) == 2, "Rust fixture pending and succeeded states"), assertion("pending", len(states) > 0 and states[0].status == m.OPERATION_STATUS_PENDING, "Rust fixture pending state"), assertion("succeeded", len(states) > 1 and states[1].status == m.OPERATION_STATUS_SUCCEEDED, "Rust fixture succeeded state")]
    wire = b"".join(item.SerializeToString() for item in states)
    put("acyclic.machines.v1.MachinesService/WatchOperation", "server", states, watch_checks, wire, {"response_count": len(states)})

log = {
    "schema": "acyclic.sdk.rpc-scenario-log.v1",
    "source_revision": revision,
    "execution_mode": "remote",
    "consumer": {"name": "acyclic-python-installed-machines-consumer", "version": "0.2.0", "artifact_path": os.environ["CONSUMER_ARTIFACT"], "artifact_sha256": os.environ["CONSUMER_SHA256"]},
    "scenarios": rows,
    "passed": sum(row["status"] == "passed" for row in rows),
    "failed": sum(row["status"] == "failed" for row in rows),
}
(out / "scenario-log.json").write_text(json.dumps(log, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"total": len(rows), "passed": log["passed"], "failed": log["failed"]}))
