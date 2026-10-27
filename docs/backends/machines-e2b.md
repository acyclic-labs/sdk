# E2B Machines

Enable `acyclic-machines/e2b`. Compose `RemoteMachines<E2b>` using explicit API
URL/key, guest domain, optional envd proxy URL and lifetime ceiling. Register a
dedicated E2B `template_id:immutable_build_uuid` in `RemoteMachinesConfig.images`, a
`NetworkPolicy::BlockAll` digest in `networks`. Supply the image's working directory
to the qualification runner. The `e2b-conformance` example shows the bindings.
Credentials are never read from an ambient profile by the adapter.

Control-plane create/delete and envd Connect execution/file I/O are transport
code; lifecycle, stream ownership, expiry, binary output and cancellation use the
shared Machines core. The template needs bash, POSIX sh, setsid,
`/bin/kill`, readable `/proc` and writable `/tmp`, and an existing workspace directory.
Only blocked egress is supported. Checkpoint, restore and disk fork use E2B's
native filesystem-only snapshot primitive. Capture pauses the sandbox at the
provider boundary; children cold boot without its memory or processes. The shared
core owns snapshot custody and admission; the adapter releases snapshots through
the template deletion API. This wire binding still needs real provider qualification.
No pause/resume or idle contract is advertised. Provider-default lifetime resolves to the configured ceiling
(one hour in the example); explicit infinite lifetime is refused. Operation records and console history are process-local. This
binding does not implement the harness kernel filesystem seam.
The shared core refuses bare templates and mutable tags before qualification or
create. E2B declares build-qualified addressing; snapshots retain their separate
native atomic handles. This adds no polling or build cache. The pinned upstream
[selector query](https://github.com/e2b-dev/runtime/blob/7278c2a380767c9989da73cf4c04b1af1b32da18/packages/db/queries/templates/get_template_with_build_by_tag.sql)
resolves UUID suffixes as exact build IDs, and its
[tag validator](https://github.com/e2b-dev/runtime/blob/7278c2a380767c9989da73cf4c04b1af1b32da18/packages/shared/pkg/id/id.go)
forbids mutable UUID tags. These are protocol sources, not runtime qualification.

## Validation and provenance

The transport was derived from the published `e2b@2.61.0` SDK's generated envd
Connect schema and sandbox REST requests. These are protocol sources, not real
response fixtures. Provider conformance has not yet run, and no response fixtures
are claimed. Source `07e558968fa4926b9262562885b5b48be2620de0` passed bb2
compile-only checks with all three provider features, and the full unfiltered
Machines library suite passed 72 tests. These checks do not qualify E2B's wire or
native snapshot behavior. An isolated Embed host or coordinator captures are required.
Shared transport and immutable image admission source
`5deac2f509b0d9581bf2a1e6d2a713d249982a12` passed the compile gate and full
conformance 24 / Machines 77 / Daytona 49 library suites on bb2. These checks
include scoped credentials and refusal before transport dispatch for mutable
template selectors. They do not qualify E2B runtime behavior.
MachineShell shares the unchanged native input schema. Supply an explicit
timeout within 1..60 s: the native default 300 s exceeds canonical Machines admission
and is refused before execution. Oversized guest arguments are refused there too.

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
   console.log(JSON.stringify({ selector: `${build.templateId}:${build.buildId}` }));
   ```

   Export the returned build-qualified selector as `E2B_TEMPLATE_ID` and
   `export E2B_WORKSPACE=/workspace`. A bare template ID or mutable tag is refused.
   For hosted E2B, perform this step in the coordinator's account and use
   `https://api.e2b.app`, domain `e2b.app`, and unset `E2B_ENVD_PROXY`.
4. Run `cargo run --locked -p acyclic-conformance --features e2b,machines-shell --example e2b-conformance`.
   Save the namespace, output, elapsed time and sanitized real control/Connect/file/snapshot/template-deletion
   traffic for a recorded-response fixture bundle, including capture time,
   source versions and hashes. Preserve frame bytes and ordering; remove only secrets.
5. Inspect the printed namespace for uncertain creates and destroy those sandboxes
   before rerunning. The example cleans up every observed machine. For Embed,
   finish with `docker compose down`; host configuration restoration belongs to
   the allocated host's owner. Do not remove shared volumes.
