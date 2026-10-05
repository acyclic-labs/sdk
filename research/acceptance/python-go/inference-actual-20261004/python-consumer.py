import hashlib
import json
import os
import sys
import grpc
from google.protobuf.json_format import MessageToDict

from acyclic_sdk.generated.inference.v1 import inference_pb2 as i
from acyclic_sdk.generated.inference.v1 import inference_pb2_grpc as ig

meta = json.load(open(os.environ["FIXTURE_META"], encoding="utf-8"))
root = meta["caCertificate"].encode()
cert = meta["certificate"].encode()
key = meta["privateKey"].encode()
creds = grpc.ssl_channel_credentials(root_certificates=root, certificate_chain=cert, private_key=key)
out = os.environ["SCENARIO_OUT"]
rev = os.environ["SOURCE_REVISION"]
os.makedirs(out, exist_ok=True)
scenarios = []

def digest_bytes(value):
    return {"byte_length": len(value), "sha256": "sha256:" + hashlib.sha256(value).hexdigest()}

def decoded_summary(value, depth=0):
    if depth > 4:
        return {"type": value.DESCRIPTOR.full_name, "truncated": True}
    fields = {}
    for field, item in value.ListFields():
        if field.message_type is not None:
            if field.is_repeated:
                fields[field.name] = [decoded_summary(child, depth + 1) for child in item[:16]]
            else:
                fields[field.name] = decoded_summary(item, depth + 1)
        elif field.is_repeated:
            if field.type == field.TYPE_BYTES:
                fields[field.name] = [digest_bytes(child) for child in item[:16]]
            elif field.enum_type is not None:
                fields[field.name] = [{"number": int(child), "name": field.enum_type.values_by_number[int(child)].name} for child in item[:16]]
            else:
                fields[field.name] = list(item[:16])
        elif field.type == field.TYPE_BYTES:
            fields[field.name] = digest_bytes(item)
        elif field.enum_type is not None:
            fields[field.name] = {"number": int(item), "name": field.enum_type.values_by_number[int(item)].name}
        else:
            fields[field.name] = item
    return {"type": value.DESCRIPTOR.full_name, "fields": fields}

def check(name, condition, detail):
    return {"name": name, "status": "passed" if condition else "failed", "detail": detail}

def expected_semantics(rpc, response):
    checks = []
    if rpc.endswith("ModelsService/List"):
        model = response.models[0] if len(response.models) == 1 else None
        checks = [check("fixture-model", model is not None and model.model == "fixture-model", "Rust fixture model identity"), check("model-limits", model is not None and model.maximum_context == 4096 and model.maximum_output == 1024, "Rust fixture limits"), check("model-features", model is not None and list(model.features) == ["generate", "stream"], "Rust fixture features")]
    elif rpc.endswith("ContextsService/Create"):
        checks = [check("revision", response.revision == b"\x01" * 32, "Rust fixture revision"), check("command-digest", response.command_digest == b"\x02" * 32, "Rust fixture command digest"), check("sequence", response.sequence == 1 and response.retained, "Rust fixture mutation receipt")]
    elif rpc.endswith("ContextsService/Inspect"):
        checks = [check("model", response.model == "fixture-model", "Rust fixture context model"), check("provenance", response.HasField("provenance") and response.provenance.HasField("created"), "Rust fixture created provenance"), check("digests", response.lineage == b"\x03" * 32 and response.execution_profile == b"\x04" * 32 and response.content_digest == b"\x05" * 32, "Rust fixture context digests")]
    elif rpc.endswith("ContextsService/Mutate"):
        checks = [check("revision", response.revision == b"\x06" * 32 and response.command_digest == b"\x07" * 32, "Rust fixture mutation digests"), check("sequence", response.sequence == 2 and not response.retained, "Rust fixture mutation receipt")]
    elif rpc.endswith("WarmContextsService/") or "WarmContextsService/" in rpc:
        expected_state = 4 if rpc.endswith("/Release") else 1
        checks = [check("warm-identity", response.commitment == b"\x08" * 32 and response.context == b"\x01" * 32, "Rust fixture warm identity"), check("warm-profiles", response.model_profile == b"\x09" * 32 and response.latency_profile == b"\x0a" * 32, "Rust fixture warm profiles"), check("warm-state", response.state == expected_state and response.sequence == 1, "Rust fixture warm state")]
    elif rpc.endswith("RunsService/Generate"):
        run = response.run if response.HasField("run") else None
        checks = [check("run-identity", run is not None and run.run_id == b"\x02" * 16, "Rust fixture run identity"), check("run-input", run is not None and run.input == b"\x03" * 32 and run.model == "model.example.v1", "Rust fixture run input"), check("run-active", run is not None and run.last_sequence == 0 and not run.cancellation_requested and not run.HasField("result"), "Rust fixture active run")]
    elif rpc.endswith("RunsService/Inspect"):
        checks = [check("run-active", response.last_sequence == 0 and not response.cancellation_requested and response.model == "model.example.v1", "Rust fixture inspected run")]
    elif rpc.endswith("RunsService/Cancel"):
        checks = [check("run-cancelled", response.last_sequence == 1 and response.cancellation_requested and response.HasField("result"), "Rust fixture cancellation"), check("cancel-terminal", response.result.terminal == 5, "Rust fixture cancelled terminal")]
    elif rpc.endswith("EvaluationsService/") or "EvaluationsService/" in rpc:
        checks = [check("evaluation-identity", response.evaluation_id == b"\x0d" * 16, "Rust fixture evaluation identity"), check("evaluation-state", response.state == 3 and response.sequence == 1, "Rust fixture completed evaluation")]
    return checks

def put(rpc, shape, response, semantic_checks=None, response_override=None):
    raw_response = response_override if response_override is not None else response.SerializeToString()
    obj = {
        "schema": "acyclic.sdk.rpc-scenario-result.v1",
        "source_revision": rev,
        "status": "passed",
        "invoked": True,
        "exit_code": 0,
        "family": "inference",
        "rpc": rpc,
        "shape": shape,
        "transport": "grpc+mtls",
        "checks": ["invocation", "transport", "serialization", "tls-hostname-verification"],
        "response_sha256": "sha256:" + hashlib.sha256(raw_response).hexdigest(),
        "response_bytes": len(raw_response),
        "decoded_summary": decoded_summary(response) if response_override is None else {"stream_items": len(response)},
        "semantic_checks": semantic_checks if semantic_checks is not None else expected_semantics(rpc, response),
    }
    name = f"python-inference-{len(scenarios)+1:02d}.json"
    raw = (json.dumps(obj, indent=2) + "\n").encode()
    open(os.path.join(out, name), "wb").write(raw)
    scenarios.append({"output_path": "qualification/consumers/" + name, "output_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()})

with grpc.secure_channel(meta["endpoint"].replace("https://", ""), creds) as channel:
    models = ig.ModelsServiceStub(channel).List(i.ListModelsRequest(), timeout=10)
    put("inference.customer.v1.ModelsService/List", "unary", models, expected_semantics("inference.customer.v1.ModelsService/List", models))

    contexts = ig.ContextsServiceStub(channel)
    identity = i.RequestIdentity(client_instance=b"\x01" * 16, request_id=b"\x02" * 16)
    created = contexts.Create(i.CreateContextRequest(identity=identity, model="fixture-model"), timeout=10)
    put("inference.customer.v1.ContextsService/Create", "unary", created, expected_semantics("inference.customer.v1.ContextsService/Create", created))
    inspected = contexts.Inspect(i.InspectContextRequest(revision=created.revision), timeout=10)
    put("inference.customer.v1.ContextsService/Inspect", "unary", inspected, expected_semantics("inference.customer.v1.ContextsService/Inspect", inspected))
    mutated = contexts.Mutate(i.MutateContextRequest(identity=identity, source=created.revision, fork=i.Empty()), timeout=10)
    put("inference.customer.v1.ContextsService/Mutate", "unary", mutated, expected_semantics("inference.customer.v1.ContextsService/Mutate", mutated))

    warm = ig.WarmContextsServiceStub(channel)
    retained = warm.Retain(i.RetainWarmRequest(identity=identity, context=created.revision, latency_profile=b"\x03" * 32, expires_at_ms=4102444800000), timeout=10)
    put("inference.customer.v1.WarmContextsService/Retain", "unary", retained, expected_semantics("inference.customer.v1.WarmContextsService/Retain", retained))
    winsp = warm.Inspect(i.InspectWarmRequest(commitment=retained.commitment), timeout=10)
    put("inference.customer.v1.WarmContextsService/Inspect", "unary", winsp, expected_semantics("inference.customer.v1.WarmContextsService/Inspect", winsp))
    renewed = warm.Renew(i.RenewWarmRequest(identity=identity, commitment=retained.commitment, expires_at_ms=4102444800000), timeout=10)
    put("inference.customer.v1.WarmContextsService/Renew", "unary", renewed, expected_semantics("inference.customer.v1.WarmContextsService/Renew", renewed))
    released = warm.Release(i.ReleaseWarmRequest(identity=identity, commitment=retained.commitment), timeout=10)
    put("inference.customer.v1.WarmContextsService/Release", "unary", released, expected_semantics("inference.customer.v1.WarmContextsService/Release", released))

    runs = ig.RunsServiceStub(channel)
    generated = runs.Generate(i.GenerateRunRequest(identity=identity, context=created.revision, input=i.Item(kind=i.ITEM_KIND_USER, payload=b"hello"), maximum_output=8), timeout=10)
    run_id = generated.run.run_id
    put("inference.customer.v1.RunsService/Generate", "unary", generated, expected_semantics("inference.customer.v1.RunsService/Generate", generated))
    inspected_run = runs.Inspect(i.InspectRunRequest(run_id=run_id), timeout=10)
    put("inference.customer.v1.RunsService/Inspect", "unary", inspected_run, expected_semantics("inference.customer.v1.RunsService/Inspect", inspected_run))
    events = list(runs.Watch(i.WatchRunRequest(run_id=run_id, from_sequence=0), timeout=10))
    watch_bytes = b"".join(e.SerializeToString() for e in events)
    watch_obj = {"events": len(events), "sha256": "sha256:" + hashlib.sha256(watch_bytes).hexdigest()}
    watch_checks = [check("watch-count", len(events) == 2, "Rust fixture emits progress and terminal events"), check("watch-progress", len(events) > 0 and events[0].sequence == 0 and events[0].HasField("progress") and events[0].progress.kind == "queued", "Rust fixture queued progress"), check("watch-terminal", len(events) > 1 and events[1].sequence == 1 and events[1].terminal == 1, "Rust fixture completed terminal")]
    put("inference.customer.v1.RunsService/Watch", "server", events, watch_checks, json.dumps(watch_obj, sort_keys=True).encode())
    cancelled = runs.Cancel(i.InspectRunRequest(run_id=run_id), timeout=10)
    put("inference.customer.v1.RunsService/Cancel", "unary", cancelled, expected_semantics("inference.customer.v1.RunsService/Cancel", cancelled))

    evals = ig.EvaluationsServiceStub(channel)
    spec = i.EvaluationSpec(
        candidates=[i.EvaluationArtifact(digest=b"\x04" * 32, media_type="text/plain", logical_size=1)],
        suite=i.EvaluationSuite(identity="fixture-suite", digest=b"\x05" * 32, cases=[i.EvaluationCase(case_id=b"\x06" * 16, input=b"x")]),
        grader=i.EvaluationGrader(handle=b"fixture", artifact_digest=b"\x07" * 32),
        metrics=[i.EvaluationMetric(identity="quality", aggregation=i.EVALUATION_AGGREGATION_MEAN)],
        maximum_case_results=1,
        spec_digest=b"\x08" * 32,
    )
    evaluation = evals.Create(i.CreateEvaluationRequest(identity=identity, spec=spec), timeout=10)
    put("inference.customer.v1.EvaluationsService/Create", "unary", evaluation, expected_semantics("inference.customer.v1.EvaluationsService/Create", evaluation))
    evaluation_view = evals.Inspect(i.InspectEvaluationRequest(evaluation_id=evaluation.evaluation_id), timeout=10)
    put("inference.customer.v1.EvaluationsService/Inspect", "unary", evaluation_view, expected_semantics("inference.customer.v1.EvaluationsService/Inspect", evaluation_view))

log = {
    "schema": "acyclic.sdk.rpc-scenario-log.v1",
    "source_revision": rev,
    "consumer": {"name": "acyclic-python-installed-inference-consumer", "version": "0.2.0", "artifact_path": os.environ["CONSUMER_ARTIFACT"], "artifact_sha256": os.environ["CONSUMER_SHA256"]},
    "scenarios": scenarios,
}
open(os.path.join(out, "scenario-log.json"), "w", encoding="utf-8").write(json.dumps(log, indent=2) + "\n")
print(f"wrote {len(scenarios)} actual Python Inference scenario results")

