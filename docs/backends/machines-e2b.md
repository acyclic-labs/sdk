# E2B Machines

Enable `acyclic-machines/e2b`. Compose `RemoteMachines<E2b>` using explicit API
URL/key, guest domain, optional envd proxy URL and lifetime ceiling. Register a
dedicated E2B template ID in `RemoteMachinesConfig.images`, a
`NetworkPolicy::BlockAll` digest in `networks`. Supply the image's working directory
to the qualification runner. The `e2b-conformance` example shows the bindings.
Credentials are never read from an ambient profile by the adapter.

Control-plane create/delete and envd Connect execution/file I/O are transport
code; lifecycle, stream ownership, expiry, binary output and cancellation use the
shared Machines core. The template needs bash, POSIX sh, setsid,
`/bin/kill`, readable `/proc` and writable `/tmp`, and an existing workspace directory.
Only blocked egress is supported. Snapshot, checkpoint restoration and fork are
currently refused pending integration of E2B's native filesystem snapshot boundary.
The published SDK pauses the sandbox during capture and restores filesystem-only
snapshots by cold boot; this primitive still needs provider qualification here.
No pause/resume or idle contract is advertised. Provider-default lifetime resolves to the configured ceiling
(one hour in the example); explicit infinite lifetime is refused. Operation records and console history are process-local. This
binding does not implement the harness kernel filesystem seam.
E2B resolves template IDs through mutable build tags. Freeze the dedicated
template's default build for this binding's lifetime and retain its build ID;
the adapter does not yet enforce an immutable build commitment.

## Validation and provenance

The transport was derived from the published `e2b@2.61.0` SDK's generated envd
Connect schema and sandbox REST requests. These are protocol sources, not real
response fixtures. Provider conformance has not yet run, and no response fixtures
are claimed. The current canonical suite requires checkpoint/fork unconditionally;
this binding refuses those vectors and cannot yet pass the complete suite. An authorized local Embed host or coordinator captures are required.

[E2B Embed](https://github.com/e2b-dev/runtime/tree/main/embed/compose) offers an
account-free Linux host. Its official Docker Compose stack alters host namespaces,
networking, NBD and hugepages, needs nested virtualization and reserves fixed ports.
Do not start it on the shared bb2 host until the coordinator allocates an isolated
window/host. The researched Compose environment pins infra commit
`7278c2a380767c9989da73cf4c04b1af1b32da18`.

## Live steps

1. On the coordinator's allocated Embed host, use the official pinned Compose
   files and prerequisites. Run `docker compose up -d --wait`, then
   `docker compose --profile test run --rm smoke`. Record host/kernel,
   orchestrator/envd versions, template ID and complete command output.
2. Load the stack's generated SDK environment privately using
   `eval "$(docker compose exec ready cat /run/e2b/sdk.env)"`.
   It supplies `E2B_API_KEY` and `E2B_API_URL`. Set
   `export E2B_ENVD_PROXY="$E2B_SANDBOX_URL" E2B_GUEST_DOMAIN=localhost`.
   Never include credentials in a recording.
3. In a separate temporary directory run `npm install --save-exact e2b@2.61.0`,
   then save and run this as `node build.mjs` using the private SDK environment:

   ```js
   import { Template } from "e2b";
   const template = Template().fromImage("debian:trixie-slim").runCmd(
     "apt-get update && apt-get install -y bash tar gzip util-linux procps && mkdir -p /workspace && chmod 0777 /workspace",
     { user: "root" },
   );
   const build = await Template.build(template, `machines-${crypto.randomUUID()}`);
   console.log({ templateId: build.templateId, buildId: build.buildId });
   ```

   Export the returned template ID as `E2B_TEMPLATE_ID` and
   `export E2B_WORKSPACE=/workspace`. Preserve the build ID and do not rebuild
   or reassign this template during the run.
   For hosted E2B, perform this step in the coordinator's account and use
   `https://api.e2b.app`, domain `e2b.app`, and unset `E2B_ENVD_PROXY`.
4. Run `cargo run --locked -p acyclic-conformance --features e2b,machines-shell --example e2b-conformance`.
   Save the namespace, output, elapsed time and sanitized real control/Connect/file
   traffic for a recorded-response fixture bundle, including capture time,
   source versions and hashes. Preserve frame bytes and ordering; remove only secrets.
5. Inspect the printed namespace for uncertain creates and destroy those sandboxes
   before rerunning. The example cleans up every observed machine. For Embed,
   finish with `docker compose down`; host configuration restoration belongs to
   the allocated host's owner. Do not remove shared volumes.
