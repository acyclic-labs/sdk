# Cloudflare Forge research

Status: pinned source review and bounded prototype, 2026-10-03

## Finding

Forge is useful as a downstream OpenAPI resolver and documentation surface,
but it is not a Rust contract engine and is not yet a safe replacement for
the canonical protobuf/gRPC path. The smallest defensible role in this
migration is:

```text
Rust descriptors + Rust route/documentation metadata
  -> generated OpenAPI HTTP projection
  -> Forge resolver/docs surface and qualified HTTP SDKs
```

Keep native protobuf/gRPC generation for streaming, cancellation, recovery,
presence, oneof, field numbers, and descriptor handshakes. Do not hand-write
an OpenAPI contract merely to satisfy Forge.

## Pinned upstream evidence

The Cloudflare Forge checkout was resolved at
`86cb1ef3047abc7441d96c894e8cd35e826fa8e5` (the `main` commit returned by
`git ls-remote` on 2026-10-03). Its root README describes Forge as an
early-days, schema-first OpenAPI generation and surface-tooling framework,
focused first on Cloudflare's `cf` CLI. The README lists TypeScript, Go,
Python, Terraform, and docs as future targets; those statements are roadmap
intent, not verified outputs.

- [Forge repository](https://github.com/cloudflare/forge)
- [Forge README at the pinned revision](https://raw.githubusercontent.com/cloudflare/forge/86cb1ef3047abc7441d96c894e8cd35e826fa8e5/README.md)
- [Forge package metadata](https://raw.githubusercontent.com/cloudflare/forge/86cb1ef3047abc7441d96c894e8cd35e826fa8e5/packages/forge/package.json)
- [Cloudflare's Forge announcement](https://blog.cloudflare.com/forge-open-source-generation-pipeline/)

The core package is `@cloudflare/forge` `0.1.0`, TypeScript, Node `>=22`,
with `openapi-format`, `openapi-types`, and `yaml` dependencies. It exports
the `Forge` host, OpenAPI resolver, overlays, and schema helpers. The source
is Apache-2.0 at the Forge repository root. The package itself has no Rust or
protobuf descriptor input path.

Forge's docs package, `astro-fern`, consumes OpenAPI and builds Astro content
for catalogs, operations, routes, Markdown/agent documents, and snippets.
Its host application supplies the theme, navigation, and snippet renderers.
That makes it a possible acyclic.dev presentation layer after generated
content is made complete; it does not author Rust comments or examples.

- [astro-fern README](https://raw.githubusercontent.com/cloudflare/forge/86cb1ef3047abc7441d96c894e8cd35e826fa8e5/packages/astro-fern/README.md)
- [Cloudflare docs-site content configuration](https://raw.githubusercontent.com/cloudflare/forge/86cb1ef3047abc7441d96c894e8cd35e826fa8e5/packages/docs-site/src/content.config.ts)
- [Cloudflare snippet target policy](https://raw.githubusercontent.com/cloudflare/forge/86cb1ef3047abc7441d96c894e8cd35e826fa8e5/packages/docs-site/src/cloudflare.ts)

The checked-in Cloudflare docs-site target list includes curl, `cf`,
TypeScript, Python, Ruby, Go, and Terraform, but its renderers currently
return snippets for curl, `cf`, TypeScript, Python, and Ruby; Go and Terraform
are marked `comingSoon`. This is a concrete distinction between an inventory
entry and a working docs target.

## Fern-backed generation path

Forge's `cloudflare-fern-config` package currently pins the Fern CLI package
at `5.112.0` and its generator matrix pins these image tags:

| Target | Image tag in Forge | Practical status in this review |
| --- | ---: | --- |
| TypeScript | `fernapi/fern-typescript-sdk:3.80.1` | configured, Docker-backed |
| Python | `fernapi/fern-python-sdk:5.18.1` | configured, Docker-backed |
| Go | `fernapi/fern-go-sdk:1.47.2` | configured, Docker-backed |
| Java | `fernapi/fern-java-sdk:4.13.2` | configured, Docker-backed |
| PHP | `fernapi/fern-php-sdk:2.11.1` | configured, Docker-backed |
| C# | `fernapi/fern-csharp-sdk:2.73.1` | configured, Docker-backed |
| Ruby | `fernapi/fern-ruby-sdk:1.15.0` | configured, Docker-backed |
| Swift | `fernapi/fern-swift-sdk:0.35.16` | configured, Docker-backed |
| Rust | `fernapi/fern-rust-sdk:0.42.1` | configured, Docker-backed |

The Forge script accepts `FORGE_OPENAPI_SPEC`, copies the supplied document
into its Fern directory, and runs `fern generate --group <lang>-sdk --local`.
This is a real OpenAPI-to-SDK route, but it requires Docker and generator
images. Forge's own package metadata has no C++, Kotlin, Dart, or language
neutral generator entry.

- [Forge Fern generation script](https://raw.githubusercontent.com/cloudflare/forge/86cb1ef3047abc7441d96c894e8cd35e826fa8e5/packages/cloudflare-fern-config/scripts/generate-sdk.ts)
- [Forge generator matrix](https://raw.githubusercontent.com/cloudflare/forge/86cb1ef3047abc7441d96c894e8cd35e826fa8e5/packages/cloudflare-fern-config/fern/generators.yml)
- [Fern self-hosted generation](https://buildwithfern.com/learn/sdks/deep-dives/self-hosted)
- [Fern generator configuration](https://buildwithfern.com/learn/sdks/reference/generators-yml)

Fern's public source repository is Apache-2.0 and contains source for its
language generators. The source revision inspected here is
`fern-api/fern` `68f0bc168c7f167734feddcfffb67eeeb982eccd`; its Rust generator
source and Dockerfile are under `generators/rust/sdk`, and the source's latest
Rust SDK version is `0.52.0`. Forge pins the older image tag `0.42.1`, so the
source HEAD is evidence of availability, not proof that it exactly reproduces
that image.

- [Fern source repository](https://github.com/fern-api/fern)
- [Fern source LICENSE](https://raw.githubusercontent.com/fern-api/fern/68f0bc168c7f167734feddcfffb67eeeb982eccd/LICENSE)
- [Fern Rust generator source](https://github.com/fern-api/fern/tree/68f0bc168c7f167734feddcfffb67eeeb982eccd/generators/rust)

The Forge Apache license does not cover Fern images, their base images, or
their bundled npm/cargo/system dependencies. A production adoption must pin
image digests, retain the exact source/image mapping, and generate an SBOM
and license notice set for every image and produced package. Fern's docs say
local generation downloads images and may make package-restore network calls;
that is a supply-chain and reproducibility input, not a reason to treat the
Forge repository license as blanket coverage.

The pinned Forge package install gives a concrete first-pass dependency
review. The lockfile resolves the core runtime to `openapi-format@1.33.6`
(MIT), `openapi-types@12.1.3` (MIT), and `yaml@2.9.0` (ISC). The installed
transitive resolver packages include `api-ref-bundler@0.5.1`,
`case-anything@2.1.10`, `json-crawl@0.4.2`, `jsonpathly@3.0.0`, and
`neotraverse@0.6.18`, all reporting MIT metadata; the development runner pins
`tsx@4.22.4` (MIT), `esbuild@0.28.2` (MIT), and `typescript@6.0.3`
(Apache-2.0). This is package metadata evidence for the isolated Forge
resolver install, not a complete image SBOM: the Fern CLI, Docker layers,
generator images, Rust toolchain, and package-restore dependencies were not
downloaded or executed here and need a separate digest-pinned license scan.

The machine-readable facts are kept in
[toolchain-provenance.json](../../research/forge/toolchain-provenance.json).
Entries distinguish verified source licenses and lockfile package metadata
from unresolved image digests, generated-package dependencies, and tools that
were not executed. A null Fern image digest is intentional evidence of an
unqualified image, not an omitted value.

## Bounded prototype

The complete record is in [research/forge/README.md](../../research/forge/README.md).
The prototype uses the existing Rust Actors projection at
`rust/crates/sdk-openapi-prototype`; no independent OpenAPI contract was
added. Its Rust tests passed (3/3), and the generated 3.0.3 document contains
eight POST routes with descriptor-derived schemas, protobuf JSON metadata,
oneof/presence extensions, bearer auth, canonical error responses, and a
source contract digest.

Against that document, Forge's pinned resolver passed these checks:

- `createActor`, `inspectActor`, and `invokeActor` resolve to their generated
  paths and request-body references;
- response keys `200` and `default` are retained;
- an in-memory metadata overlay builds one `actors` command with all eight
  methods and returns operation descriptions.

The direct resolver probe is reproducible from `target/forge-upstream` after
the Rust projection command completes:

```text
node --import tsx --input-type=module -e "import { readFileSync } from 'node:fs'; import { populateOperationMap, resolveOperation } from './packages/forge/index.ts'; const d=JSON.parse(readFileSync('../../rust/crates/sdk-openapi-prototype/target/actors.openapi.json','utf8')); populateOperationMap(d); for (const id of ['createActor','inspectActor','invokeActor']) { const op=resolveOperation(id); console.log(JSON.stringify({id,path:op?.path,method:op?.method,requestBodyRef:op?.requestBodyRef,responses:Object.keys(op?.responses??{})})); }"
```

The tracked, dependency-free [Forge preflight probe](../../research/forge/probe.py)
is the repeatable gate before invoking Forge. It reports operation-description
coverage, unresolved local schema references, response presence, protobuf RPC
identities, descriptor provenance, and counts for protobuf JSON/presence/
oneof/enum extensions. Its tests cover a valid projection, an empty operation
description, a bad `$ref`, and missing RPC identity:

```text
python research/forge/probe.py rust/crates/sdk-openapi-prototype/target/actors.openapi.json --strict
python -m unittest discover -s research/forge -p 'test_probe.py' -v
```

The current Actors run reports eight operations and eight RPC identities, but
zero description coverage, so strict mode exits non-zero as intended. It also
reports 44 local schema references and the expected contract-derived
protobuf extension counts. This lightweight Python harness is a downstream
comparison gate; it does not become the SDK/docs generator or replace the
Rust source of truth.

The Rust contract model already carries method documentation metadata, while
the current OpenAPI emitter has not yet copied it into operation
`description` fields. The strict failure is therefore a concrete handoff to
the OpenAPI/docs owner rather than evidence that Forge should own prose.

The overlay run first failed because the Rust projection currently emits no
operation descriptions. Forge intentionally rejects all eight operations for
that input. Supplying temporary descriptions makes it pass, proving that the
missing data belongs in the Rust-owned documentation layer rather than in a
second hand-authored API contract.

Forge's own resolver suite also passed: 36 tests, 0 failures, using Node
`v24.4.1` and pnpm `10.13.1` in the ignored pinned checkout.

The Fern SDK/docs generation command was not claimed as executed. Docker
client `29.7.2` is installed, but `docker info` cannot connect to the Linux
daemon (`dockerDesktopLinuxEngine` is unavailable). The limitation is
recorded rather than counted as successful generation.

### Docker-free feasibility

The pinned Forge core is practical without Docker: its OpenAPI resolver and
metadata tests ran in an isolated Node checkout, and `Forge.transform()` can
feed a local downstream transformer. The tracked Python preflight provides a
dependency-free gate before that optional comparison. This is source and
resolver feasibility only; it does not qualify an SDK or website artifact.

The official Fern path is different. Forge's generation script invokes
`fern generate --local`, which requires Docker and the configured generator
images. With the daemon unavailable, there is no honest Docker-free Fern SDK
qualification in this loop. A future run can either enable Docker and pin
image digests, or qualify a separately pinned OpenAPI Generator Java artifact
against the same Rust projection; neither route changes the Rust contract
decision. Astro docs can be built from generated content once its own
dependencies and renderers receive the same provenance and SBOM treatment.

## Fit and decision

### Architecture selection

| Candidate role | Evidence in this run | Selection |
| --- | --- | --- |
| Rust/protobuf contract and native clients | Existing descriptors preserve wire identities and the Rust Actors projection passes its fidelity tests. | **Authoritative core** |
| Forge `@cloudflare/forge` resolver/overlays | 36 upstream resolver tests pass; Rust-derived routes resolve, but descriptions are required and input remains OpenAPI. | **Adopt downstream for qualified HTTP/docs surfaces** |
| Forge Fern multi-language generation | Nine configured image targets, but Docker daemon and images were unavailable; no package artifact was produced. | **Qualify later; no success claim** |
| Forge as universal SDK source | No protobuf/gRPC input in the pinned package; no verified stream/recovery or embedded behavior path. | **Reject** |
| Forge Astro docs as acyclic.dev source | `astro-fern` can consume generated OpenAPI and host-rendered snippets, but source examples/descriptions must originate in Rust. | **Use only as generated presentation layer** |

This selection satisfies the source-of-truth requirement: Rust owns the
contract, documentation metadata, and examples; Forge is replaceable
downstream machinery. It also gives a bounded exit path if Forge's target
coverage, Docker supply chain, or docs fidelity fails qualification.

Use Forge only after the Rust generator emits operation descriptions,
availability, status mappings, examples, and any required snippet metadata.
Feed it a reproducible OpenAPI projection with source revision, descriptor
hash, generator versions, and output hashes. Keep the OpenAPI projection
limited to operations whose HTTP/JSON semantics are explicit.

Do not use Forge as the source for protobuf field numbers, enum identities,
proto3 optional presence, oneof semantics, gRPC streaming, cancellation,
recovery, idempotency, or embedded Rust behavior. Those remain in Rust and
the protobuf/gRPC descriptor pipeline. Qualify each Fern target with package
installation, serialization, auth, transport, streaming/recovery, and docs
snippet tests before removing the corresponding handwritten TypeScript
surface.

The next bounded milestone is to add Rust-owned descriptions and examples to
the projection, run Forge's resolver/docs package with Docker enabled, and
compare generated artifacts and hashes. Until then, Forge is a promising
downstream experiment with a verified resolver, not the selected universal
SDK generator.
