# Decoder and format inventory

This inventory joins wire identity, persisted format versions, golden bytes,
fuzz entry points and bounded proofs. The protobuf inventory is generated from
`acyclic_proto_build::FAMILIES`: every message, including nested messages, has
a typed entry in `acyclic-codec-fuzz::DECODERS`. A protobuf package version owns
its RPC route; framing and semantic decoders are separate boundaries below.

Fuzzing and proofs run on demand. A source entry is not evidence of an executed
campaign or proof. Captured corpus provenance is `rust/codec-fuzz/corpus/SOURCES.tsv`;
descriptor baselines live in `compatibility/descriptors/`. Golden payloads belong
in `compatibility/frames/`, alongside their producing encoder and refusal cases.

| Boundary | Canonical implementation | Version admission | Fuzz entry | Golden / proof work remaining |
| --- | --- | --- | --- | --- |
| Auth RPC messages | `auth::contract` | `acyclic.auth.v1` route | `protobuf` | Payload goldens |
| Actors RPC messages | `actors::wire` | `acyclic.actors.v1` route | `protobuf` | Payload goldens |
| Filesystem RPC messages | `filesystem-wire::wire` | `acyclic.filesystem.v2` route and service contract version | `protobuf` | Payload goldens |
| Inference RPC messages | `inference::wire` | `inference.customer.v1` route | `protobuf` | Payload goldens |
| Machines RPC messages | `machines::wire` | `acyclic.machines.v1` route | `protobuf` | Payload goldens |
| Objects RPC messages | `objects::wire` | `acyclic.objects.v1` route | `protobuf` | Payload goldens |
| Stream RPC messages | `stream::wire` | `acyclic.stream.v1` route | `protobuf` | Payload goldens |
| Protobuf JSON projections | `protobuf-json`, service JSON codecs | Same versioned routes | `protobuf_json` | Capture and refusals |
| Session admission | `stream::session`, client handshake | Hello/Welcome version 2; initial interest shares Hello | `protobuf`, existing session target | Central admission/refusal golden; execute |
| Session delimited/gRPC frames | `stream::wire_codec::session::FrameDecoder` | Admitted session owns version | `session_frames` | Kani prefix/EOF/progress harnesses; execute and capture |
| Stream commands | `stream::journal` | `ACYCLIC-STREAM-COMMAND-V1` | `stream_persistence` | Capture and version refusals |
| Stream retained observations/envelopes | `stream::persistence` | Observation/envelope domains, version 1 | `stream_persistence` | Capture, central goldens and refusal execution |
| Stream change records | `stream::changes` | `changes/v2` marker; v1 and unknown markers refused | `stream_persistence` | Central retained-v1 refusal golden; current capture |
| Stream memory snapshots | `stream::memory::snapshot` | `ACYCLIC-STREAM-STATE-V2` | `stream_persistence` | Capture and version refusals |
| Local journal header/frames | `stream::local::{frames,journal_frame}` | `ACYCLIC-STREAM-LOCAL-V2`; nested command v1 | `stream_persistence` | Capture and version refusals |
| Local snapshot container | `stream::local::snapshot` | `ACYCLIC-STREAM-SNAP-V02` | `stream_persistence` | Capture and refusal |
| Local incarnation | `stream::local::snapshot` | `ACYCLIC-STREAM-INCARNATION-V1` | `stream_persistence` | Capture and refusal |
| Objects manifest | `objects::blob::Manifest` | `acyclic-manifest-v1` | `object_persistence` | Central golden capture |
| Objects HEAD metadata | `objects::blob::Envelope` | `acyclic-blob-v1`; obsolete layouts refused | `object_persistence` | Central golden capture |
| Objects retained-reference batch | `objects::blob::decode_batch` | `acyclic-retained-v1` | `object_persistence` | Central golden capture |
| Objects incarnation/epoch | `objects::blob` | `acyclic-incarnation-v1`, `acyclic-epoch-v1` | `object_persistence` | Central golden; execute |
| ObjectId, all eight kinds | `filesystem::storage::ObjectId` | Typed tag and exact 33/113-byte width | `ids` | Six ID Kani harnesses; consumer codec audit (G96), execute |
| GenerationId | `filesystem::foundation::GenerationId` | Root ObjectId codec; raw 32 bytes refused | `ids` | Same proofs, sealed locator preservation (G96) |
| Fixed-width UUIDs/digests/CommitId | Defining types' `from_bytes`/`to_bytes` | Fixed-width fields under enclosing formats | ID proofs | Captured values |
| Auth capability and signed blocks | `auth::{authority,verify}` | Capability version 1 before verification | `auth` | Signed-token capture and refusal golden |
| Auth caveats/grants/record attributes | `auth::{authority,contract}` | Enclosing capability/record schema | `auth`, `protobuf` | Semantic attribute coverage and goldens |
| Resource and Actor identities | `auth::addressing` | Resource pair currently unversioned; Actor reference owns framing | `identities` | Capture, version policy and bounded proof |
| Actor state | `actors::local::State` | `ff AST 01` prefix | `actor_codecs` | Golden and typed version refusal |
| Actor guest manifest/delivery/transition | `actors::guest::wire` | Guest ABI v1; independent messages lack explicit admission | `actor_codecs` | Version envelopes and goldens |
| Actor invocation/timer/reservation records | `actors::{invocation,timers,reservation}` | Mixed retained domains and JSON codecs | `actor_codecs` covers timers; others pending | Complete per-record version audit and capture |
| Filesystem authority/durable records | `filesystem::streams_record` | Version 1 domains and u16; typed unsupported version | `filesystem_codecs` | Capture and refusal goldens |
| Filesystem state and companion records | `filesystem::{distributed,workspace_context,lazy_workspace,git_compat,lineage}` | Existing versioned domains; individual admission audit pending | Pending | Typed version refusals and captures |
| Filesystem core journal and snapshots | `filesystem::core_state` | Persisted framing audit pending | Pending private-codec hook | Capture and refusals |
| Filesystem join/control projections | `filesystem::{compat_wire,workspace_context_wire}` | Versioned control schema; semantic validation | `filesystem_codecs` | Capture and refusals |
| Filesystem immutable pages and roots | `filesystem::{kernel,storage}` | Eight-byte domains and u16 version 1; typed unknown-version refusal | `filesystem_pages`; existing `decoder_work` | Central goldens; private file-record/source-state hooks |
| Filesystem native checkpoint | `filesystem::windows_usn` | `ACYUSN 00 01` | Pending portable codec hook | Capture and refusals |
| Machine journal records | `machines::provider::journal` | JSON envelope version `1` | Pending private-codec hook | Capture and typed version refusal |
| Harness retained records/checkpoints | `harness-core::{agent,context,conversation}` | Record v2 and format-specific guards | Pending | Complete retained-record inventory and captures |
| Harness wire and streaming transports | `harness-core::{wire_codec,mcp,model}` | Own protocol/version negotiation, including external protocols | Pending | Boundary-specific parsing and captures |

Version checks must precede payload interpretation and side effects. A refused
version never triggers migration, repair, truncation or rewrite. Tests mutate a
captured version and truncate its body to establish this ordering. Invalid input,
trailing input, EOF and owner loss are distinct negative controls where applicable.

Fixed-width codecs do constant work. Variable payload codecs necessarily take
work proportional to the admitted payload; their cost must not depend on retained
history, object count or unrelated agents. Byte parsers perform no syscalls,
durability barriers, round trips, waits or polling. Session version admission
uses the same first flight as the first interest and adds no negotiation request.
