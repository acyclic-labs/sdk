import asyncio, json, os
import hashlib
from pathlib import Path

FIXTURE = Path(os.environ.get("ACYCLIC_FIXTURE_OPTIONS", r"Q:\sdk\work\go-remote-primitive-current\fixture-options.json"))

def check_actor(value, expected):
    assert value is not None
    assert value.actor_id.value() == expected["actorId"]
    assert bytes(value.code_sha256.value()) == bytes(expected["codeSha256"])
    assert value.home_region == expected["homeRegion"]
    assert value.state.value == expected["state"]
    assert value.checkpoint_epoch == expected["checkpointEpoch"]
    assert value.configuration_revision == expected["configurationRevision"]

async def main():
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    receipt_path = os.environ.get("ACYCLIC_QUALIFICATION_RECEIPT")
    if receipt_path:
        # Do not leave a prior PASS receipt behind when this run fails.
        Path(receipt_path).unlink(missing_ok=True)
    import acyclic_actors_uniffi as m

    operations = []
    checks = []
    client = await m.connect_actors_with_ca(
        fixture["endpoint"],
        fixture["token"],
        fixture["caCertificate"].encode("utf-8"),
        None,
    )
    actor = m.ActorId(fixture["actorId"])
    digest = m.CodeSha256(bytes(fixture["expectedObservation"]["codeSha256"]))
    binding = m.Binding("binding-a", "capability-a", "resource-a")
    limits = m.ActorLimits(1, 2, 3)
    start = m.SubscriptionStart.CURSOR(9007199254740993)
    subscription = m.SubscriptionSpec("subscription-a", "events/input", start, True)
    create = m.CreateActorRequest(digest, "eu", [binding], limits, [subscription], "create-a")
    update = m.UpdateActorRequest(actor, digest, [binding], limits, 0, "update-a")
    inspect = m.InspectActorRequest(actor)
    add = m.AddSubscriptionRequest(actor, subscription, "add-a")
    remove = m.RemoveSubscriptionRequest(actor, "subscription-a", "remove-a")
    resume = m.ResumeSubscriptionRequest(actor, "subscription-a", "resume-a")
    checkpoint = m.CheckpointActorRequest(actor, "checkpoint-a")
    invoke = m.InvokeActorRequest(
        actor, "POST", "/invoke", b"request-body",
        [m.Header(name="content-type", value="application/json")],
    )
    expected = fixture["expectedObservation"]
    for operation, request in (
        ("create", create),
        ("update", update),
        ("inspect_actor_request", inspect),
        ("add_subscription", add),
        ("remove_subscription", remove),
        ("resume_subscription", resume),
        ("checkpoint_actor", checkpoint),
    ):
        method = getattr(client, operation)
        result = await method(request, None)
        check_actor(result, expected)
        operations.append(operation)
    response = await client.invoke_actor(invoke, None)
    assert response.status == 201
    assert response.body == b""
    assert [(h.name, h.value) for h in response.headers] == [("location", "/result")]
    operations.append("invoke")
    checks.append("remote")
    unauthorized = await m.connect_actors_with_ca(
        fixture["endpoint"], "wrong-token", fixture["caCertificate"].encode("utf-8"), None,
    )
    try:
        await unauthorized.inspect_actor(actor, None)
    except m.BindingError.Service as error:
        assert error.grpc_code == 16
        assert error.service_code is None
        assert error.detail_message == "service rejected operation"
    else:
        raise AssertionError("unauthorized operation did not return typed service error")
    checks.append("service_error")
    cancelled = m.CancellationHandle()
    cancelled.cancel()
    try:
        await client.inspect_actor(actor, cancelled)
    except m.BindingError as error:
        assert "cancel" in type(error).__name__.lower() or "cancel" in str(error).lower()
    else:
        raise AssertionError("pre-cancelled operation completed")
    checks.append("cancellation")
    if receipt_path:
        receipt = {
            "schema": "acyclic.language-package.qualification/v1",
            "status": "PASS",
            "operations": operations,
            "checks": checks,
            "fixture_options_sha256": hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
        }
        artifact_path = os.environ.get("ACYCLIC_ARTIFACT_PATH")
        if artifact_path:
            artifact = Path(artifact_path)
            receipt["artifact"] = {
                "path": str(artifact),
                "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                "bytes": artifact.stat().st_size,
            }
        Path(receipt_path).write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print("Installed all-eight Python wheel remote conformance: PASS")

asyncio.run(main())







