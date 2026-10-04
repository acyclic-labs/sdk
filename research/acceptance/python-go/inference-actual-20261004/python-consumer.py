import hashlib
import json
import os
import sys
import grpc

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

def put(rpc, shape, response):
    raw_response = response.SerializeToString()
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
    }
    name = f"python-inference-{len(scenarios)+1:02d}.json"
    raw = (json.dumps(obj, indent=2) + "\n").encode()
    open(os.path.join(out, name), "wb").write(raw)
    scenarios.append({"output_path": "qualification/consumers/" + name, "output_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()})

with grpc.secure_channel(meta["endpoint"].replace("https://", ""), creds) as channel:
    models = ig.ModelsServiceStub(channel).List(i.ListModelsRequest(), timeout=10)
    put("inference.customer.v1.ModelsService/List", "unary", models)

    contexts = ig.ContextsServiceStub(channel)
    identity = i.RequestIdentity(client_instance=b"\x01" * 16, request_id=b"\x02" * 16)
    created = contexts.Create(i.CreateContextRequest(identity=identity, model="fixture-model"), timeout=10)
    put("inference.customer.v1.ContextsService/Create", "unary", created)
    inspected = contexts.Inspect(i.InspectContextRequest(revision=created.revision), timeout=10)
    put("inference.customer.v1.ContextsService/Inspect", "unary", inspected)
    mutated = contexts.Mutate(i.MutateContextRequest(identity=identity, source=created.revision, fork=i.Empty()), timeout=10)
    put("inference.customer.v1.ContextsService/Mutate", "unary", mutated)

    warm = ig.WarmContextsServiceStub(channel)
    retained = warm.Retain(i.RetainWarmRequest(identity=identity, context=created.revision, latency_profile=b"\x03" * 32, expires_at_ms=4102444800000), timeout=10)
    put("inference.customer.v1.WarmContextsService/Retain", "unary", retained)
    winsp = warm.Inspect(i.InspectWarmRequest(commitment=retained.commitment), timeout=10)
    put("inference.customer.v1.WarmContextsService/Inspect", "unary", winsp)
    renewed = warm.Renew(i.RenewWarmRequest(identity=identity, commitment=retained.commitment, expires_at_ms=4102444800000), timeout=10)
    put("inference.customer.v1.WarmContextsService/Renew", "unary", renewed)
    released = warm.Release(i.ReleaseWarmRequest(identity=identity, commitment=retained.commitment), timeout=10)
    put("inference.customer.v1.WarmContextsService/Release", "unary", released)

    runs = ig.RunsServiceStub(channel)
    generated = runs.Generate(i.GenerateRunRequest(identity=identity, context=created.revision, input=i.Item(kind=i.ITEM_KIND_USER, payload=b"hello"), maximum_output=8), timeout=10)
    run_id = generated.run.run_id
    put("inference.customer.v1.RunsService/Generate", "unary", generated)
    inspected_run = runs.Inspect(i.InspectRunRequest(run_id=run_id), timeout=10)
    put("inference.customer.v1.RunsService/Inspect", "unary", inspected_run)
    events = list(runs.Watch(i.WatchRunRequest(run_id=run_id, from_sequence=0), timeout=10))
    watch_bytes = b"".join(e.SerializeToString() for e in events)
    watch_obj = {"events": len(events), "sha256": "sha256:" + hashlib.sha256(watch_bytes).hexdigest()}
    put("inference.customer.v1.RunsService/Watch", "server", type("Response", (), {"SerializeToString": lambda self: json.dumps(watch_obj, sort_keys=True).encode()})())
    cancelled = runs.Cancel(i.InspectRunRequest(run_id=run_id), timeout=10)
    put("inference.customer.v1.RunsService/Cancel", "unary", cancelled)

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
    put("inference.customer.v1.EvaluationsService/Create", "unary", evaluation)
    evaluation_view = evals.Inspect(i.InspectEvaluationRequest(evaluation_id=evaluation.evaluation_id), timeout=10)
    put("inference.customer.v1.EvaluationsService/Inspect", "unary", evaluation_view)

log = {
    "schema": "acyclic.sdk.rpc-scenario-log.v1",
    "source_revision": rev,
    "consumer": {"name": "acyclic-python-installed-inference-consumer", "version": "0.2.0", "artifact_path": os.environ["CONSUMER_ARTIFACT"], "artifact_sha256": os.environ["CONSUMER_SHA256"]},
    "scenarios": scenarios,
}
open(os.path.join(out, "scenario-log.json"), "w", encoding="utf-8").write(json.dumps(log, indent=2) + "\n")
print(f"wrote {len(scenarios)} actual Python Inference scenario results")

