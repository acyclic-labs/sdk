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
    artifact_identity = None
    artifact_path = os.environ.get("ACYCLIC_ARTIFACT_PATH")
    producer_manifest_path = os.environ.get("ACYCLIC_PRODUCER_MANIFEST")
    producer_manifest = None
    if receipt_path:
        if not artifact_path:
            raise AssertionError("ACYCLIC_ARTIFACT_PATH is required when emitting a qualification receipt")
        if not producer_manifest_path:
            raise AssertionError(
                "ACYCLIC_PRODUCER_MANIFEST is required when emitting a qualification receipt"
            )
        producer_manifest = json.loads(
            Path(producer_manifest_path).read_text(encoding="utf-8")
        )
        required_manifest_fields = (
            "source_revision",
            "source_inventory_sha256",
            "uniffi_bindgen",
            "uniffi_source_sha256",
            "python_patch_sha256",
        )
        missing_manifest_fields = [
            field for field in required_manifest_fields if not producer_manifest.get(field)
        ]
        if missing_manifest_fields:
            raise AssertionError(
                "producer manifest is missing: " + ", ".join(missing_manifest_fields)
            )
        artifact = Path(artifact_path)
        artifact_identity = {
            "path": str(artifact),
            "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            "bytes": artifact.stat().st_size,
        }
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
        ("create_actor", create),
        ("update_actor", update),
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
    checks.append("pre-cancelled")
    pending_fixture_path = os.environ.get("ACYCLIC_PENDING_FIXTURE_OPTIONS")
    if pending_fixture_path:
        pending_fixture = json.loads(Path(pending_fixture_path).read_text(encoding="utf-8"))
        pending_client = await m.connect_actors_with_ca(
            pending_fixture["endpoint"],
            pending_fixture["token"],
            pending_fixture["caCertificate"].encode("utf-8"),
            None,
        )
        marker_path = os.environ.get("ACYCLIC_PENDING_MARKER_PATH")
        if not marker_path:
            raise AssertionError("ACYCLIC_PENDING_MARKER_PATH is required with a pending fixture")
        marker = Path(marker_path)
        task = asyncio.create_task(pending_client.inspect_actor(actor, None))
        for _ in range(200):
            if marker.exists() and "request path=" in marker.read_text(encoding="utf-8", errors="replace"):
                break
            await asyncio.sleep(0.01)
        else:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise AssertionError("pending fixture never observed the in-flight request")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("in-flight asyncio.Task.cancel did not cancel the request")
        # Cross-host relays can delay the marker after the client has already
        # cancelled the task. Allow five seconds for the server-side close
        # observation while still requiring the marker from this run.
        for _ in range(500):
            if "stream-close" in marker.read_text(encoding="utf-8", errors="replace"):
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("server did not observe stream close after task cancellation")
        checks.extend(("in-flight-task-cancellation", "server-abort-cleanup"))
    if receipt_path:
        artifact = Path(artifact_path)
        artifact_after = {
            "path": str(artifact),
            "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            "bytes": artifact.stat().st_size,
        }
        if artifact_after != artifact_identity:
            raise AssertionError("artifact identity changed during qualification")
        receipt = {
            "schema": "acyclic.language-package.qualification/v1",
            "status": "PASS",
            "operations": operations,
            "checks": checks,
            "fixture_options_sha256": hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),
            "source_revision": producer_manifest["source_revision"],
            "source_inventory_sha256": producer_manifest["source_inventory_sha256"],
            "toolchain": {
                "uniffi_bindgen": producer_manifest["uniffi_bindgen"],
                "uniffi_source_sha256": producer_manifest["uniffi_source_sha256"],
                "python_patch_sha256": producer_manifest["python_patch_sha256"],
            },
        }
        # The language-package model consumes the artifact under its language
        # key.  Keep the producer's measured identity; never copy a caller's
        # hash into a different output record.
        receipt["artifacts"] = {"wheel": artifact_identity}
        Path(receipt_path).write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print("Installed all-eight Python wheel remote conformance: PASS")

asyncio.run(main())







