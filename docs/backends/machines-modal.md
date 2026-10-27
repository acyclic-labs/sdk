# Modal Machines

Enable `acyclic-machines/modal` and compose `RemoteMachines<Modal>`. Construct
`Modal` with an explicit gRPC API URL, token ID/secret and app ID. Register the
immutable built image ID in `RemoteMachinesConfig.images`, a blocked-egress
policy digest in `networks`. Supply `/workspace` to the qualification runner. See the
`modal-conformance` example. The adapter does not read Modal profiles.

The adapter projects the published Modal Sandbox V2 control-plane and task-command
router RPCs. The shared core owns lifecycle, output, process-group cancellation,
and expiry. Checkpoint, restore and disk fork use Modal's native immutable
filesystem-image snapshot boundary. The core owns snapshot custody, cancellation,
expiry and child admission; the adapter requests 30-day retention and releases
images with `ImageDelete`. These RPC bindings still need captured-real-response
qualification. Images need bash, POSIX sh, setsid, `/bin/kill`,
readable `/proc` and writable `/tmp` and an existing workspace. Blocked egress and explicit
domain allowlists are supported. The example allocates one CPU and 1 GiB RAM.
Provider-default lifetime resolves to 24 hours; infinite lifetime, memory forks,
suspend/resume and idle policy are refused. Expired router credentials produce an
unavailable result; automatic credential refresh is not implemented. Operation records and console history are process-local. This
binding does not implement the harness kernel filesystem seam.

## Validation and provenance

RPC field numbers and service names come from the published `modal@0.11.1`
generated client/schema. That schema is protocol source, not captured responses.
Modal has no local sandbox emulator in that client. Provider conformance and a
recorded-real-response replay remain pending coordinator capture; no fabricated
response server is used. Source `07e558968fa4926b9262562885b5b48be2620de0`
passed bb2 compile-only checks with all three provider features, and the full
unfiltered Machines library suite passed 72 tests. These checks do not qualify
Modal's wire or native snapshot behavior.
Shared transport and immutable image admission source
`25830279bd2648d01670c6b6ebe00176967426b6` passed the compile gate and full
conformance 24 / Machines 77 / Daytona 49 library suites on bb2. These checks
include scoped credentials and refusal before transport dispatch for mutable
template selectors, plus shared RPC input pumping. Job
`1793098004380-166798-be025746`, rc0, all four build cleanup fields true.
They do not qualify Modal runtime behavior.
MachineShell shares the unchanged native input schema. Supply an explicit
timeout within 1..60 s: the native default 300 s exceeds canonical Machines admission
and is refused before execution. Oversized guest arguments are refused there too.

## Live steps

1. In the coordinator's isolated Modal account, provide `MODAL_TOKEN_ID` and
   `MODAL_TOKEN_SECRET` securely. Install `modal@0.11.1` in a temporary Node project.
2. Build a dedicated app/image with the official client:

   ```js
   import { ModalClient } from "modal";
   const modal = new ModalClient({
     tokenId: process.env.MODAL_TOKEN_ID,
     tokenSecret: process.env.MODAL_TOKEN_SECRET,
   });
   try {
     const app = await modal.apps.fromName("machines-conformance", { createIfMissing: true });
     const image = await modal.images.fromRegistry("debian:trixie-slim")
       .dockerfileCommands(["RUN apt-get update && apt-get install -y bash tar gzip util-linux procps && mkdir -p /workspace"])
       .build(app);
     console.log(JSON.stringify({ app: app.appId, image: image.imageId }));
   } finally { modal.close(); }
   ```

3. Export those returned IDs as `MODAL_APP_ID` and `MODAL_IMAGE_ID`. From the
   repository root run
   `cargo run --locked -p acyclic-conformance --features modal,machines-shell --example modal-conformance`.
4. Preserve the namespace, full output and elapsed time. Capture sanitized real
   SandboxCreateV2/WaitV2/TerminateV2, ImageDelete, and router
   TaskSnapshotFilesystem/Start/StdioRead/Wait/StdinWrite
   responses, with endpoint roles, versions, capture time, hashes and frame ordering.
   Exclude credentials, JWTs and personal data. Return that provenance-bearing bundle
   to the lane for the unchanged suite's recorded-response run.
5. The runner destroys all observed machines. Inspect the namespace in Modal for
   uncertain creates before rerunning and remove the dedicated app/image when done.
