import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {dirname} from 'node:path';
import {fileURLToPath} from 'node:url';

// Every Loom model source is classified here. A real model drives the
// production component through its crate shim, so any lock it constructs or
// loom lock type it names needs a stated reason that the lock is the
// production one under test (or a labelled test-environment observer). An
// abstract model states the boundary it substitutes. There is no substitute
// grade: a model that reimplements component state belongs on a real-path
// fixture instead. A schedule capped at a numeric preemption bound states that
// bound and cannot claim exhaustive exploration.
export const models = {
  "rust/crates/harness/src/hooks/effects/models.rs": { grade: "abstract", scope: "Actual prepared hook claims, task-volume index, codecs and CAS; the model Journal replaces provider IO with a single journal topology. Not disk durability." },
  "rust/crates/harness/src/local/checkout_tools/hooks/models.rs": { grade: "abstract", scope: "Actual serialized/with_workers guard retention and shim; hook process completion is a synthetic worker and cleanup flag. Not real subprocess cleanup." },
  "rust/crates/harness/src/local/children/controller/draining/models.rs": { grade: "abstract", scope: "Actual child drain/finish paths and cancellation tokens; leaf signal lock and synthetic futures replace external effect IO. Not executor or filesystem completion." },
  "rust/crates/harness/src/local/children/controller/drive/models.rs": { grade: "abstract", scope: "Actual during() driver; a shim mutex and synthetic metadata jobs represent the coordinator owner. Not the complete coordinator implementation." },
  "rust/crates/harness/src/local/children/controller/follow/models.rs": { grade: "abstract", scope: "Actual follow/control refresh and task-volume file queries on MemoryStream; admission is a supplied future. Not child executor admission or persistent IO." },
  "rust/crates/harness/src/local/children/controller/inputs/models.rs": { grade: "abstract", scope: "Actual input acknowledgement/publication paths on task files and MemoryStream; supplied futures stand in for live child execution. Not executor effects." },
  "rust/crates/harness/src/local/children/controller/stopping/models.rs": { grade: "abstract", scope: "Actual controller stop/drain path and task files; synthetic effect future and accepted control cut exclude publication/follow registration races and disk IO." },
  "rust/crates/harness/src/local/children/preparation/models.rs": { grade: "abstract", scope: "Actual fenced_inputs() and cancellation wakeup; synthetic flusher/commit futures and an atomic witness replace native staged writes. Not filesystem durability." },
  "rust/crates/native-runtime/src/apple_model.rs": { grade: "real" },
  "rust/crates/actors/src/guest/state.rs": { grade: "real", constructs: "production State constructor in this source file" },
  "rust/crates/actors/tests/timer_loom.rs": { grade: "real" },
  "rust/crates/filesystem/fuzz/tests/model.rs": { grade: "real" },
  "rust/crates/filesystem/src/cache/decoded_model.rs": { grade: "real" },
  "rust/crates/filesystem/src/core_state/owners/models.rs": { grade: "real" },
  "rust/crates/filesystem/src/demand/admissions_loom_tests.rs": { grade: "abstract", scope: "Actual NativeProofAccounting; explicit modeled reference count substitutes for std Arc strong_count semantics. Reclamation protocol only." },
  "rust/crates/filesystem/src/demand/context/model_checking.rs": { grade: "real" },
  "rust/crates/filesystem/src/demand/context_loom_tests.rs": { grade: "real", constructs: "test-environment callback result mutex/condvar, acquired only after the leaf drops and released before any demand call" },
  "rust/crates/filesystem/src/demand/ownership_loom_tests.rs": { grade: "abstract", scope: "Actual retirement functions/atomics; explicit owner counts and publication witness. Does not instrument every Arc destruction site." },
  "rust/crates/filesystem/src/distributed/head_payload_loom_tests.rs": { grade: "abstract", scope: "Actual StreamAuthorityStore head_payload_checked and MemoryStream; ModelStream injects retirement/epoch/cancellation cuts." },
  "rust/crates/filesystem/src/git_compat/tests/model.rs": { grade: "real" },
  "rust/crates/filesystem/src/kernel/allocation/accounting/charge_model.rs": { grade: "real" },
  "rust/crates/filesystem/src/kernel/codec/accounting/model_checking.rs": { grade: "abstract", scope: "Real EncodeAccounting/reservations; synthetic observer/effect callbacks, refunds and nested aggregate reservations. Not all malformed decoder inputs." },
  "rust/crates/filesystem/src/kernel/file_read/accounting/model_checking.rs": { grade: "abstract", scope: "Real read accounting/reservation/refund locks; copy steps are supplied explicitly. Does not drive the complete file decoder." },
  "rust/crates/filesystem/src/kernel/path_access/cache/loom_tests.rs": { grade: "real" },
  "rust/crates/filesystem/src/kernel/tests/persistent_io/owned_loom.rs": { grade: "real" },
  "rust/crates/filesystem/src/lazy_workspace/hash_loom_tests.rs": { grade: "real" },
  "rust/crates/filesystem/src/lock_order/checked/model.rs": { grade: "real", constructs: "the production checked Mutex and Condvar are the component under test" },
  "rust/crates/filesystem/src/materializer/allocation_model.rs": { grade: "real" },
  "rust/crates/filesystem/src/native_capture/sparse_scan_loom.rs": { grade: "real" },
  "rust/crates/filesystem/src/native_mount/fuse/invalidation/model_checking.rs": { grade: "abstract", scope: "Actual FUSE invalidator/waiter and shim locks; fake kernel replies and elapsed backoff. Does not prove real timed wait or kernel delivery." },
  "rust/crates/filesystem/src/native_mount/disconnected/models.rs": { grade: "abstract", scope: "Actual MountDestinationGuard registry/fence acquisition and disconnected-scratch recovery; connectivity, flock, helper exit and mount syscalls are a modeled kernel. Does not prove real FUSE or kernel mount behavior." },
  "rust/crates/filesystem/src/native_mount/view_ledger/model_checking.rs": { grade: "real", constructs: "production record() receives the view-ledger RwLock it maintains" },
  "rust/crates/filesystem/src/workspace/identity_loom.rs": { grade: "real" },
  "rust/crates/filesystem/tests/native_binding_model.rs": { grade: "abstract", scope: "Real cached-binding predicate/lease-order helper and ProbeGuard; source lease/stamp/cache environment is handwritten. Not mounted callback proof." },
  "rust/crates/filesystem/tests/native_stage_loom.rs": { grade: "abstract", scope: "Real StageState phase lock; atomic journal acceptance and physical effects are a handwritten Store. Does not prove journal implementation or crash durability." },
  "rust/crates/harness-providers/tests/models/cancellation.rs": { grade: "real" },
  "rust/crates/harness-providers/tests/models/refresh.rs": { grade: "abstract", scope: "Production refresh::execute included with shim; HTTP/file IO replaced by small futures. Covers failed-attempt sharing, not credential ACLs, JSON bounds or full dispatch." },
  "rust/crates/harness-providers/tests/verification_models.rs": { grade: "real" },
  "rust/crates/harness-tui/src/term/console_owner.rs": { grade: "abstract", scope: "Actual console owner state/locks/worker handoff; modeled native submit/cancel/restore actions. Does not run ConPTY or Windows console API." },
  "rust/crates/harness-tui/src/term/lifecycle.rs": { grade: "abstract", scope: "Actual terminal lifecycle/state/locks and close/resume fencing; Vec/fake restore IO. Does not prove OS restoration or signal integration." },
  "rust/crates/harness/src/client/commands/model.rs": { grade: "runner" },
  "rust/crates/harness/src/client/commands/session_confirmation/tests.rs": { grade: "real" },
  "rust/crates/harness/src/client/commands/session_init_models.rs": { grade: "real" },
  "rust/crates/harness/src/client/mailbox_model.rs": { grade: "real" },
  "rust/crates/harness/src/distributed/tests/model_checking.rs": { grade: "abstract", scope: "Actual distributed publication/coordinator and MemoryStream locks; PublicationCut/provider callbacks bound the external effect cut." },
  "rust/crates/harness/src/distributed/tests/scheduler_model.rs": { grade: "abstract", scope: "Actual distributed coordinator/settlement; modeled provider/content effects and fixed publication cuts. Not host effect recovery." },
  "rust/crates/harness/src/local/checkout_tools/tests.rs": { grade: "abstract", scope: "Actual checkout serialization/custody/cache-selection helpers; generation/cursor/task state and branch objects are toy values. Not complete shell/mount publication." },
  "rust/crates/harness/src/local/checkout_tools/tests/fencing.rs": { grade: "real" },
  "rust/crates/harness/src/local/compact/checkpoints/model.rs": { grade: "real" },
  "rust/crates/harness/src/local/inheritance/model.rs": { grade: "abstract", scope: "Actual inheritance prepare/retention/ancestry/control files and real MemoryStream; owner admission itself is an injected mutex-backed parent receipt." },
  "rust/crates/harness/src/local/workspace/bootstrap/loom_tests.rs": { grade: "abstract", scope: "Actual initialize future; namespace preparation/publish/discard are handwritten array callbacks. Does not prove real directory rename/fsync behavior." },
  "rust/crates/harness/src/local/workspace/projection/tests.rs": { grade: "abstract", scope: "Actual projection initialize/serialization functions and shim; fake projection construction and tuple checkout state. Not mounted OS calls." },
  "rust/crates/harness/src/mcp/install/cancellation.rs": { grade: "real" },
  "rust/crates/harness/src/mcp/stdio/native/codec.rs": { grade: "real" },
  "rust/crates/harness/src/search/files/model.rs": { grade: "abstract", scope: "Actual search worker, TaskVolume and MemoryStream; fake search provider. Explicit preemption bound 2, not an unqualified exhaustive result.", bound: "every scenario runs at two preemptions; a green run witnesses the listed controls only and is not an exhaustive schedule result" },
  "rust/crates/harness/src/task_volume/files/models.rs": { grade: "real" },
  "rust/crates/harness/src/task_volume/page.rs": { grade: "real" },
  "rust/crates/harness/src/task_volume/publication/models.rs": { grade: "real" },
  "rust/crates/harness/src/workflow/model_checking/tests.rs": { grade: "real" },
  "rust/crates/harness/tests/memory_accounting_model.rs": { grade: "real", constructs: "production owned_scan receives the shared work/allocation mutex" },
  "rust/crates/harness/tests/query_model.rs": { grade: "real", constructs: "query_sync shim Mutex/RwLock split-update controls prove the modeled primitives themselves; projection families use production TaskVolume locks", bound: "five families run at four preemptions; assert_negative_control certifies each fault first fails below that bound, which is a fault witness, not completeness" },
  "rust/crates/harness/tests/task_volume/durability_model.rs": { grade: "abstract", scope: "Actual TaskVolume durability/refresh/index code; custom Provider supplies histories, replacement and suspended IO. Not a persistent store crash proof." },
  "rust/crates/native-runtime/src/testing/model.rs": { grade: "real", constructs: "production Context detail slot assembled by the test constructor" },
  "rust/crates/native-runtime/src/testing/queue_model.rs": { grade: "real", constructs: "production NativeWorkers receiver and admission locks assembled by the model" },
  "rust/crates/objects/src/v1/memory/transaction/model.rs": { grade: "abstract", scope: "Actual MemoryObjects mutate_locked_engine/apply/get_locked, state/receipt locks and lock ranks; fake durability callbacks. No disk crash/fsync claim." },
  "rust/crates/stream/models/cancellation.rs": { grade: "real" },
  "rust/crates/stream/models/checked.rs": { grade: "real" },
  "rust/crates/stream/models/credit.rs": { grade: "real", constructs: "test-environment receiver storage, never locked while dropping a delivery or taking the production signal/cursor lock" },
  "rust/crates/stream/models/gate.rs": { grade: "real" },
  "rust/crates/stream/models/native_registration.rs": { grade: "real" },
  "rust/crates/stream/models/outbound.rs": { grade: "real" },
  "rust/crates/stream/models/session_model.rs": { grade: "real" },
  "rust/crates/stream/models/signal.rs": { grade: "real" },
  "rust/crates/stream/publication-models/change.rs": { grade: "real" },
  "rust/crates/stream/publication-models/cursor.rs": { grade: "real" },
  "rust/crates/stream/publication-models/interest.rs": { grade: "real" },
  "rust/crates/stream/publication-models/shared.rs": { grade: "real", constructs: "production shared() receives the client slot it initializes" },
  "rust/crates/stream/src/local/compaction_model.rs": { grade: "real" },
  "rust/crates/stream/src/local/discovery_model.rs": { grade: "real" },
  "rust/crates/stream/src/local/follow_model.rs": { grade: "real" },
  "rust/crates/stream/src/local/query_model.rs": { grade: "real", constructs: "assembles the production LocalStream/LocalInner fields, including their locks, over an in-memory journal", bound: "the accepted-query scenario takes its preemption cap from its caller; memory/recovery_model.rs runs it at four with a minimal-bound control" },
  "rust/crates/stream/src/memory/browser_follow_model.rs": { grade: "real" },
  "rust/crates/stream/src/memory/discovery_model.rs": { grade: "real" },
  "rust/crates/stream/src/memory/recovery_model.rs": { grade: "real", bound: "only the local accepted-query family is capped (four preemptions, minimal-bound control); the other families run without a preemption cap" },
  "rust/crates/stream/src/session/client/admissions.rs": { grade: "real", constructs: "production Admissions constructor in this source file" },
  "rust/crates/stream/src/session/client/cache.rs": { grade: "real", constructs: "production client cache constructor in this source file" },
  "rust/crates/stream/src/session/client/pool/discovery_model.rs": { grade: "abstract", scope: "Actual Pool subscription/Outbound queue; handwritten discovery delivery signal, fake tonic channel. Snapshot/live paths parameterized; driver itself is not run." },
  "rust/crates/stream/src/session/client/pool/model.rs": { grade: "abstract", scope: "Actual Pool routes, cursor guards, dials, interest and fault isolation; fake node/channel fixtures. Does not exercise real HTTP transport or TLS." },
  "rust/crates/stream/src/session/client/pool/model/cancellation.rs": { grade: "real" },
  "rust/crates/stream/src/session/client/pool/model/follow_data.rs": { grade: "abstract", scope: "Actual Pool latest delivery, cursor fence and watermark path; synthetic frames and channel. Does not prove complete browser recovery." },
  "rust/crates/stream/src/session/client/pool/retry_model.rs": { grade: "abstract", scope: "Actual initial retry executor path; injected Clock and gate. Proves yielding at that boundary, not real timer duration." },
  "rust/crates/stream/src/session/client/progress/model.rs": { grade: "real" },
  "rust/crates/stream/src/session/connection.rs": { grade: "real", constructs: "production connection-generation constructor in this source file" },
  "rust/crates/stream/src/session/sources/model.rs": { grade: "real" },
  "rust/crates/stream/src/session/sources/model/visibility.rs": { grade: "real" },
  "rust/verification/sdk-fixtures/model.rs": { grade: "abstract", scope: "Actual parallel join, cache/package/concrete-version helpers included; OS file lock replaced by Loom mutex and compiler/IO callbacks abstracted. Several scenarios explicitly bounded at 2.", bound: "every scenario runs at two preemptions; a green run witnesses the listed controls only and is not an exhaustive schedule result" },
  "typescript/packages/stream/models/schedules.rs": { grade: "external", scope: "Loom enumerates external event orders only, then session_model.rs replays them through real JS/WASM driver. No JS or Tokio lock is instrumented by this model." },
};

const entry = /\bloom::model\b|\bcheck_with_bound\(|preemption_bound/;
// Any bound other than a literal `None` counts, whether a number, a constant
// or a forwarded parameter: a named bound is still a bound.
const numericBound = /preemption_bound\s*=\s*(?!None\b)\S|(?<!fn\s+)\bcheck_with_bound\(\s*(?!None\b)[^\s)]|assert_negative_control\(\s*\d+\s*,\s*\d/;
const exhaustive = /exhaust|every (?:interleaving|schedule)|all (?:interleavings|schedules)/i;
const grades = new Set(['real', 'abstract', 'external', 'runner']);

// Comments and `const _: fn(Shim) -> loom::sync::Mutex` identity assertions
// do not state a bound; strip them before looking for one.
const code = source => source
  .split('\n')
  .filter(line => !/^\s*(?:\/\/|const _: fn\()/.test(line))
  .join('\n');

// Lock ownership is decided on parsed Rust, never on text: the scanner in
// rust/verification/loom-locks resolves lock aliases and Loom lock imports,
// finds every constructor spelling, and decides context-typed constructors
// (`Default::default()`, `.into()`, ...) on their expected type. Generated
// code is excluded: a real model may use only allow-listed macros and
// attributes, and never macro_rules!.
const repository = dirname(dirname(fileURLToPath(import.meta.url)));
const verdicts = new Map();
let scannerPath;

function scanner() {
  scannerPath ??= execFileSync('cargo', [
    'build', '--locked', '--quiet', '--message-format=json',
    '--manifest-path', `${repository}/rust/verification/loom-locks/Cargo.toml`,
    '--target-dir', `${process.env.CARGO_TARGET_DIR ?? `${repository}/target`}/loom-locks`,
  ], {encoding: 'utf8', maxBuffer: 1 << 26}).split('\n').filter(Boolean).map(line => JSON.parse(line))
    .find(message => message.reason === 'compiler-artifact' && message.executable)?.executable;
  if (!scannerPath) throw new Error('loom-locks scanner did not build an executable');
  return scannerPath;
}

// Judges every source not judged yet in one scanner process.
function scan(sources) {
  const pending = [...new Set(sources)].filter(source => !verdicts.has(source));
  if (!pending.length) return;
  const output = execFileSync(scanner(), [], {
    input: pending.map((source, index) => `${index}\0${source}\0`).join(''),
    encoding: 'utf8',
    maxBuffer: 1 << 26,
  });
  for (const line of output.split('\n').filter(Boolean)) {
    const [index, verdict] = line.split('\t');
    verdicts.set(pending[Number(index)], verdict);
  }
}

// '1' when the source constructs or imports a lock, '0' when not, and
// '!reason' when it does not parse, uses generated code, or is indeterminate.
function ownsLock(source) {
  scan([source]);
  return verdicts.get(source);
}

export const rules = {
  unregistered: (path, source, model) => model ? [] : [`${path}: Loom model is not classified in scripts/check-loom-models.mjs`],
  grade: (path, source, model) => !model || grades.has(model.grade) ? [] : [`${path}: grade ${model.grade} is not real, abstract, external or runner`],
  scope: (path, source, model) => ['abstract', 'external'].includes(model?.grade) && !(model.scope?.trim().length >= 20)
    ? [`${path}: ${model.grade} model does not state the boundary it substitutes`] : [],
  locks: (path, source, model) => {
    if (model?.grade !== 'real') return [];
    const verdict = ownsLock(source);
    if (verdict?.startsWith('!')) return [`${path}: lock scan rejected the model: ${verdict.slice(1)}`];
    const owned = verdict === '1';
    if (owned && !model.constructs) return [`${path}: real model defines its own lock without naming the production lock it is`];
    if (!owned && model.constructs) return [`${path}: stale lock reason; the model constructs no lock`];
    return [];
  },
  bound: (path, source, model) => {
    const bounded = numericBound.test(code(source));
    if (bounded && !model?.bound) return [`${path}: preemption bound without a stated bounded claim`];
    if (!bounded && model?.bound) return [`${path}: stale bound statement; the model has no preemption bound`];
    return [];
  },
  exhaustive: (path, source) => numericBound.test(code(source))
    ? source.split('\n').filter(line => exhaustive.test(line) && !/bound/i.test(line))
      .map(line => `${path}: unconditional exhaustive claim in a bounded model: ${line.trim()}`)
    : [],
  missing: path => [`${path}: classified Loom model is missing or no longer has a model entry`],
};

// `sources` maps each repository .rs path to its contents.
export function violations(sources, registry = models, active = rules) {
  const found = [];
  const discovered = new Set();
  if (active.locks) scan([...sources].filter(([path, source]) => entry.test(source) && registry[path]?.grade === 'real').map(([, source]) => source));
  for (const [path, source] of sources) {
    if (!entry.test(source)) continue;
    discovered.add(path);
    for (const [name, rule] of Object.entries(active)) {
      if (name !== 'missing') found.push(...rule(path, source, registry[path]));
    }
  }
  for (const path of Object.keys(registry)) {
    if (!discovered.has(path) && active.missing) found.push(...active.missing(path));
  }
  return found;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const root = repository;
  const files = execFileSync('git', ['ls-files', '-z', '--', '*.rs'], {cwd: root, encoding: 'utf8'}).split('\0').filter(Boolean);
  const sources = new Map(files.map(path => [path, readFileSync(`${root}/${path}`, 'utf8').replaceAll('\r\n', '\n')]));
  const found = violations(sources);
  if (found.length) {
    console.error(found.join('\n'));
    process.exit(1);
  }
  console.log(`${Object.keys(models).length} Loom models classified`);
}
