# Modal Machines

Enable `acyclic-machines/modal` and compose `RemoteMachines<Modal>`. Construct
`Modal` with an explicit gRPC API URL, token ID/secret and app ID. Register the
immutable built image ID in `RemoteMachinesConfig.images`, a blocked-egress
policy digest in `networks`. Supply `/workspace` to the qualification runner. See the
`modal-conformance` example. The adapter does not read Modal profiles.

The adapter projects the published Modal Sandbox V2 control-plane and task-command
router RPCs. The shared core owns lifecycle, output, process-group cancellation,
and expiry. Snapshot, checkpoint restoration and fork are refused until an atomic
provider boundary or an admitted immutable filesystem generation is available. Images need bash, POSIX sh, setsid, `/bin/kill`,
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
response server is used. The current canonical suite requires checkpoint/fork
unconditionally; this binding refuses those vectors and cannot yet pass the complete suite.

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
   SandboxCreateV2/WaitV2/TerminateV2 and router Start/StdioRead/Wait/StdinWrite
   responses, with endpoint roles, versions, capture time, hashes and frame ordering.
   Exclude credentials, JWTs and personal data. Return that provenance-bearing bundle
   to the lane for the unchanged suite's recorded-response run.
5. The runner destroys all observed machines. Inspect the namespace in Modal for
   uncertain creates before rerunning and remove the dedicated app/image when done.
