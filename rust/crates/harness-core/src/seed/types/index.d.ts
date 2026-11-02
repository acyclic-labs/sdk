// acyclic API v7.1. Read-only declarations at sys/types/index.d.ts.
// Paths:  .volumes/home/ yours; .volumes/shared/ your team's; live/agents/self/ your live state;
//   live/agents/<n>/ child <n>, whose own files are at live/agents/<n>/home/; sys/; anything else: the project.
// live/ is read-only except approval answers and your descendants' home/. Agent names: your children, "self", "parent"; "a/b" is a grandchild,
// "g/<n>" a child in group g, and a group's name targets every member. A group and a child cannot share a name.
// You may change everything about yourself except sys/, live/, your permissions and your budget; a child never gets
// more rights than you. Each settings write is confirmed in live/agents/self/admissions/<version>.json (`Admission`).
// Records on disk use these types' field names (generated from the harness's types). `At<P>` is the record at a path.

/* Files -------------------------------------------------------------- */

export type Path = string;
/** Opaque file version; pass it as `ifVersion`. */
export type Version = string & { readonly __brand: "Version" };
/** "30s", "5m", "2h", or milliseconds. */
export type Duration = `${number}${"s" | "m" | "h"}` | number;

/** An `ifVersion` or `check` failed; nothing was written. `current` is the file now (null if absent). */
export class ConflictError extends Error { readonly name: "ConflictError"; readonly path: Path; readonly current: File | null }
/** The timeout passed first. What you waited on keeps running. */
export class TimeoutError extends Error { readonly name: "TimeoutError" }
/** A before-hook or your permissions refused. Nothing was written. */
export class DeniedError extends Error { readonly name: "DeniedError"; readonly reason: string }

export interface File<T = unknown> {
  readonly path: Path;
  readonly version: Version;
  /** Bytes. */
  readonly size: number;
  text(): string;
  /** Reads only these lines (0-based). */
  lines(from: number, count: number): string[];
  json(): T;
}
export interface Entry { readonly name: string; readonly path: Path; readonly kind: "file" | "dir"; readonly version: Version }

/** The record type stored at a path (unknown for project files). Use a literal tail: `${id}/request.json`, not `${p}`. */
export type At<P extends string> =
  P extends `${string}/status.json` ? Status
  : P extends `${string}/admissions/${string}.json` ? Admission
  : P extends `${string}/approvals/${string}/request.json` ? Approval
  : P extends `${string}/approvals/${string}/answer.json` ? Answer
  : P extends `${string}/calls/${string}/call.json` ? CallRecord
  : P extends `${string}/usage.jsonl` ? Usage
  : P extends `${string}/quiescent.jsonl` ? Quiescent
  : P extends `${string}/context.json` ? ContextInfo
  : P extends `${string}/context/parts/${string}.json` ? ContextPartInfo
  : P extends `${string}/context.ts` ? ContextPlan
  : P extends `${string}/schedules/${string}.ts` ? Schedule
  : P extends `${string}/schedules/${string}.json` ? ScheduleState
  : P extends `${string}/policy.ts` ? Policy
  : P extends `${string}/plugin.ts` ? PluginManifest
  : P extends `${string}.conflict.json` ? Conflict
  : P extends `${string}.resolve.json` ? Resolution
  : unknown;
/** What `write` takes: the record (a .ts path stores `export default <value>`), else text or JSON. */
export type DataAt<P extends string> = unknown extends At<P> ? string | object : At<P>;

/** Null if absent. */
export function read<P extends Path>(path: P): Promise<File<At<P>> | null>;
/** Writes the whole file. ifVersion: "any" = unconditional; "absent" = only if missing; a Version = only if unchanged. */
export function write<P extends Path>(path: P, data: DataAt<P>, ifVersion: Version | "absent" | "any"): Promise<Version>;
/** Appends one record to a `.jsonl` log; returns its 0-based position. */
export function append<P extends Path>(path: P, record: unknown extends At<P> ? object : At<P>): Promise<number>;
export function remove(path: Path, ifVersion: Version | "any"): Promise<void>;

/** A directory's entries, or a `.jsonl` log's records, a page at a time as you iterate. Never assume one page. */
export function list<P extends Path>(path: P, after?: Cursor): Listing<P extends `${string}.jsonl` ? At<P> : Entry>;
export type Cursor = string & { readonly __brand: "Cursor" };
export interface Listing<T> extends AsyncIterable<T> {
  /** Where the items read so far end; pass it as `after` in a later step. */
  readonly cursor: Cursor;
}

export type Change =
  | { write: Path; data: string | object; ifVersion: Version | "absent" | "any" }
  | { append: Path; record: object }
  | { remove: Path; ifVersion: Version | "any" }
  /** Changes nothing; the set fails unless `check` is still at `version` (for files you may only read). */
  | { check: Path; version: Version | "absent" }
  | ({ spawn: string } & Child)
  | ({ fork: string } & Copy)
  | { send: Target; message: string | object; deliver: Deliver }
  /** After-hooks only: show the agent a message at its next step. */
  | { notice: Notice }
  /** After-hooks only: start a call (recorded and charged like any other). */
  | { [A in Action]: { call: A; input: Actions[A][0] } }[Action]
  | { call: ToolRef; input: object };

/** All or none, each through the same hooks and checks as the single call (ConflictError, DeniedError). */
export function atomically(changes: Change[]): Promise<void>;

/* Waiting ------------------------------------------------------------ */

/** What changed. `dir` and `log` carry at most one page since the last delivery. */
export type Delivery =
  /** version null = removed. */
  | { readonly kind: "file"; readonly path: Path; readonly version: Version | null; readonly size: number }
  | { readonly kind: "log"; readonly path: Path; readonly count: number; readonly added: unknown[] }
  | { readonly kind: "dir"; readonly path: Path; readonly count: number; readonly changes: { readonly name: string; readonly change: "added" | "updated" | "removed" }[] };

export type Condition =
  /** At least this many records or entries. */
  | { count: number }
  | { exists: true }
  /** A JSON file's dotted field ("result.passed") equals the value. */
  | { field: string; equals: string | number | boolean | null };

/**
 * Resolves with the first delivery meeting the condition (now is checked first); TimeoutError if `timeout` passes first.
 * As your code's result (`export default until(...)`) it survives restarts and costs nothing while waiting.
 */
export function until(path: Path, test: Condition, timeout?: Duration): Promise<Delivery>;

/* Agents ------------------------------------------------------------- */

export interface Agent {
  /** Relative to you: "reviewer-1", "fix/pkg-a", "lead/worker-3". */
  readonly name: string;
  readonly live: Path;
}
/** An agent, its name, or a group's name (every member). */
export type Target = Agent | string;

/**
 * working: running a step or tool · waiting: on something it started (call, child, timer) · blocked: needs an answer ·
 * paused: by its parent, or a fork not yet sent anything · idle: ended its turn, nothing unread; NOT success, read its
 * results · done: GOAL.md met · failed: unrecoverable error · archived: stopped for good, files still readable.
 */
export type AgentState = "working" | "waiting" | "blocked" | "paused" | "idle" | "done" | "failed" | "archived";

/** `live/agents/<n>/status.json` */
export interface Status {
  readonly state: AgentState;
  /** One line from the agent. */
  readonly summary: string;
  readonly children: Readonly<Record<AgentState, number>>;
  /** USD. `spent`: its own calls only (a fork starts at 0). `limit` null: shares its parent's. */
  readonly budget: { readonly spent: number; readonly limit: number | null };
  readonly updated: string;
}

export interface Child {
  /** Under templates/: "worker", "advisor", "reviewer". */
  template: string;
  prompt: string;
  /** USD taken from yours, or null to share yours. */
  budget: number | null;
  /** Files for its `.volumes/home/` ("AGENT.md", "agent.ts", "plugins.ts", "policy.ts", "plugins/x/..."), in place before it starts. */
  files?: Record<string, string>;
}
/** Creates a child, started once `prompt` and `files` are in place. The same name and fields again returns it; other fields: ConflictError. */
export function spawn(name: string, child: Child): Promise<Agent>;

export interface Copy {
  budget: number | null;
  /** Files written over the copy's `.volumes/home/` before it starts (as in spawn). */
  files?: Record<string, string>;
}
/**
 * Copies you now (conversation, files, settings) into a paused child; your code carries on. The copy does not re-run
 * this code (its copy of the call ends with `{ forkedAs: name }`); your first `send` starts it.
 */
export function fork(name: string, copy: Copy): Promise<Agent>;

/**
 * idle: when its turn ends (now if idle); work and cache undisturbed, the cheapest and usual choice.
 * steer: at its next step, mid-turn; costs a little cache. interrupt: stops its current step now; for "stop" or urgent.
 */
export type Deliver = "idle" | "steer" | "interrupt";
/** Wakes it if idle. */
export function send(to: Target, message: string | object, deliver: Deliver): Promise<void>;

/**
 * No new step starts in the target's subtree until `resume`; running steps finish, messages still queue, files stay
 * readable. Resolves when no step there runs. Pausing yourself or an ancestor: DeniedError.
 */
export function pause(target: Target): Promise<void>;
/** Queued messages are then handled in order. */
export function resume(target: Target): Promise<void>;
export function archive(target: Target): Promise<void>;

/**
 * Resolves when no target is working (idle, blocked, done, failed or archived, nothing unread). Says nothing about
 * success: read the results they wrote. TimeoutError if `timeout` passes first; they keep running. A fork never sent
 * anything: DeniedError. Make it your code's result, `export default wait(...)`: it then survives restarts and costs
 * nothing. A wait mid-code keeps the code running, and ends "interrupted" if the harness must stop it.
 */
export function wait(targets: Target | Target[], timeout?: Duration): Promise<void>;

export interface MergePlan {
  /**
   * `live/agents/self/merge/<id>/`: list `clean/` and `conflicts/`. A text conflict is the file with <<<<<<< ======= >>>>>>>
   * markers: edit it. Any other is `<file>.conflict.json`: write `<file>.resolve.json` beside it. Resolve all before `publish`.
   */
  readonly path: Path;
  readonly clean: number;
  readonly conflicts: number;
}
export interface Conflict {
  readonly path: Path;
  readonly kind: "binary" | "deleted" | "type" | "mode";
  /** null where deleted. */
  readonly ours: Version | null;
  readonly theirs: Version | null;
}
export type Resolution = { take: "ours" | "theirs" } | { remove: true } | { content: string };
/** Prepares bringing its project changes into your tree; nothing changes until `publish`. */
export function merge(from: Target): Promise<MergePlan>;
/** Applies the plan at once; ConflictError (nothing applied) if either tree changed since: merge again. */
export function publish(plan: MergePlan): Promise<void>;

/**
 * `live/agents/self/approvals/<id>/request.json`: a before-hook's question about one exact request by another agent (never your
 * own). New ones wake you. Answer by writing `answer.json` beside it with "absent"; ConflictError = already answered.
 */
export interface Approval {
  readonly id: string;
  readonly from: string;
  readonly question: string;
  readonly request: Request;
  readonly asked: string;
}
export interface Answer { allow: boolean; reason: string }

/* Calls -------------------------------------------------------------- */

export type CallResult<T = unknown> =
  | { readonly status: "ok"; readonly value: T }
  | { readonly status: "error"; readonly error: string }
  | { readonly status: "cancelled" }
  /** It may or may not have acted (e.g. crashed mid-call). Check before retrying. */
  | { readonly status: "unknown" };
/** `live/agents/self/calls/<id>/call.json` */
export interface CallRecord<T = unknown> {
  readonly id: string;
  readonly target: string;
  readonly input: object;
  readonly started: string;
  /** null while running. */
  readonly result: CallResult<T> | null;
}
export interface CallHandle<T> extends PromiseLike<CallResult<T>> {
  readonly id: string;
  /** `live/agents/self/calls/<id>/` */
  readonly path: Path;
  cancel(): Promise<void>;
}
/** [input, output]; the same names on every provider. */
export interface Actions {
  exec: [{ command: string; cwd?: Path; timeout?: Duration }, { exitCode: number; output: string }];
  read: [{ path: Path }, { content: string }];
  write: [{ path: Path; content: string }, {}];
  edit: [{ path: Path; old: string; new: string }, {}];
  search: [{ pattern: string; path?: Path }, { matches: { path: Path; line: number; text: string }[] }];
  web: [{ url: string } | { query: string }, { content: string }];
  /** Summarises a span of your context parts into a file for context.ts. */
  summarize: [{ from: string; to: string; instructions?: string }, { path: Path; covers: [from: string, to: string] }];
}
export type Action = keyof Actions;
/** A function a tool module exports: "tools/lint.ts#run". */
export type ToolRef = `${string}.ts#${string}`;
/** Runs an action or tool. Await it, or not: it keeps running after your code ends, its result at `handle.path`. */
export function call<T extends Action | ToolRef>(
  target: T, input: T extends Action ? Actions[T][0] : object,
): CallHandle<T extends Action ? Actions[T][1] : unknown>;

/* Settings files ------------------------------------------------------- */

/**
 * agent.ts, plugins.ts, provider.ts and plugin `config` export a value or a function of the defaults, annotated:
 * `export default (base: AgentConfig): AgentConfig => ({ ...base, model: "..." })`. policy.ts, context.ts and schedules
 * are plain values. Static imports only; no live state, clock or network; the result is plain data.
 */
export type Config<T> = T | ((base: T) => T);

/**
 * `live/agents/self/admissions/<version>.json`, for each write of a settings file, context.ts or a schedule. Confirm with
 * `export default until(`live/agents/self/admissions/${v}.json`, { exists: true })`. "invalid" and "rejected" keep the
 * previous good settings; "stale": a context plan named parts that changed.
 */
export interface Admission {
  readonly path: Path;
  readonly version: Version;
  readonly status: "applied" | "invalid" | "rejected" | "stale";
  readonly error: string | null;
  /** context.ts: the context revision it produced. */
  readonly revision: number | null;
  readonly at: string;
}

/** `.volumes/home/agent.ts` (a child's: `live/agents/<n>/home/agent.ts`). A model your permissions disallow is "rejected". */
export interface AgentConfig {
  /** "<provider>/<model>" from sys/models/. */
  model: string;
  effort: "low" | "medium" | "high" | null;
  sampling: { temperature: number | null; topP: number | null; maxOutputTokens: number | null };
  /** Run inside another agent host instead of the built-in loop. */
  host: "builtin" | "claude-code" | "codex" | "pi";
  /** Replay the recording at this path instead of a live model. */
  replay: Path | null;
}

/** To change a plugin's code, copy its folder into .volumes/home/plugins/ under a new name. */
export interface PluginEntry<C = Record<string, unknown>> {
  enabled: boolean;
  /** A folder elsewhere, pinned by content (`acyclic digest <path>`). */
  source?: { path: Path; digest: string };
  /** Its settings; the only place to change them. */
  config?: Config<C>;
}
export interface CompactionConfig {
  /** Fraction of the context window at which you are told; null = never. */
  notify: number | null;
  /** Reuse summaries of shared history from ancestors, or also siblings. */
  reuse: "ancestors" | "subtree";
}
export interface PluginConfigs {
  compaction: CompactionConfig;
  goal: { maxLoops: number | null; ignoreSchedules: boolean };
  /** Approvals are off unless enabled. */
  approvals: { mode: "ask" | "policy" | "model" | "parent" };
}
/**
 * `.volumes/home/plugins.ts`; each entry may be a function of the entry below. Applies to you and your forks. Your
 * before-hooks also check every request below you; descendants cannot turn them off. For one child, edit
 * `live/agents/<n>/home/plugins.ts` (deny its writes there in its policy.ts to make it stick).
 */
export type Plugins = { [K in keyof PluginConfigs]?: Config<PluginEntry<PluginConfigs[K]>> } & {
  [name: string]: Config<PluginEntry<any>> | undefined;
};
/** `plugins/<name>/plugin.ts` */
export interface PluginManifest {
  name: string;
  description: string;
  requires?: string[];
  defaults?: Record<string, unknown>;
  /** What each `hooks/after-<n>.{ts,sh}` watches, keyed by <n>. "start": also what is there already. */
  after?: Record<string, { path: Path; from: "start" | "now" }>;
}

/**
 * `.volumes/home/policy.ts`. Written by you: restricts you, and you may always change it. Written by an issuer (your
 * parent or above, e.g. at `live/agents/<n>/home/policy.ts` or via spawn `files`): binds you and is read-only to you. Allowed
 * unless denied here or above. `path` is in the subject's tree; for "spawn" it matches child names, for "call" actions
 * or tool paths. Notes only: `{ only: [{ path: ".volumes/home/plans/**", actions: ["write"] }] }`.
 */
export interface Policy {
  deny?: Rule[];
  /** Anything no `only` rule matches is denied. */
  only?: Rule[];
}
export interface Rule { path: string; actions: ("read" | "write" | "append" | "spawn" | "call")[] }

/** `.volumes/home/schedules/<n>.ts`: a 5-field cron in an IANA timezone, or once `at` (ISO 8601). Remove it to stop. */
export type Schedule = ({ cron: string; timezone: string } | { at: string }) & { to: string; message: string | object };
/**
 * How each firing arrives; no occurrence arrives twice, and rewriting a schedule keeps counting. Armed schedules count
 * as activity (see the goal plugin's `ignoreSchedules`).
 */
export interface ScheduledMessage { readonly schedule: string; readonly occurrence: number; readonly scheduledAt: string; readonly message: string | object }
/** `live/agents/self/schedules/<n>.json` */
export interface ScheduleState { readonly next: string | null; readonly occurrences: number; readonly lastFired: string | null }

/* Context ------------------------------------------------------------ */

/** `live/agents/self/context.json` */
export interface ContextInfo {
  /** +1 whenever your context is replaced (compaction, model change). */
  readonly revision: number;
  /** Tokens. */
  readonly capacity: number;
  /** Fraction at which the compaction plugin tells you; null = off. */
  readonly notifyAt: number | null;
  /** Past this, the harness drops the oldest turns itself. */
  readonly autoTrimAtTokens: number;
  readonly current: { readonly request: string; readonly totalTokens: number };
  readonly lastCall: { readonly request: string; readonly totalTokens: number; readonly cachedReadTokens: number | null; readonly cacheWriteTokens: number | null };
  /** `live/agents/self/context/parts/` */
  readonly parts: Path;
}
/** `live/agents/self/context/parts/<id>.json`, in order. An "exchange" (a message and its tool calls) is kept or dropped whole. */
export interface ContextPartInfo {
  readonly id: string;
  readonly kind: "instructions" | "exchange" | "summary" | "pinned";
  readonly tokens: number;
  /** A summary's replaced parts. */
  readonly covers: [from: string, to: string] | null;
}
export type ContextPart = { keep: [from: string, to: string] } | { summary: Path; covers: [from: string, to: string] } | { pin: Path };
/** `.volumes/home/context.ts`, applied at your next step: `next` covers every part up to `cut`, in order, once; later parts stay. */
export interface ContextPlan { cut: string; next: ContextPart[] }

/* Hooks: plugins/<name>/hooks/before-<event>.{ts,sh} and after-<n>.{ts,sh} ----------- */

/** `live/agents/self/usage.jsonl`, one per model call. USD. */
export interface Usage {
  readonly revision: number;
  readonly model: string;
  /** Including cached. */
  readonly inputTokens: number;
  readonly cachedTokens: number;
  readonly outputTokens: number;
  readonly capacity: number;
  readonly cost: number;
  /** From the price list, null if unknown. summariseNow: one summary call plus one resend; perCallIfNot: each call without. */
  readonly compaction: { readonly summariseNow: number | null; readonly perCallIfNot: number | null; readonly autoTrimAtTokens: number };
}
/** `live/agents/self/quiescent.jsonl`: everything below you stopped, or its armed-schedule count changed meanwhile. */
export interface Quiescent {
  /** Below you, waiting outside the team (a call, a person). */
  readonly waiting: number;
  readonly schedulesArmed: number;
  readonly at: string;
}

/**
 * Shown at the agent's next step. wake: also wake it if idle (never for changes it caused). once: only the first use of
 * this key in the change's context revision; notices about an older revision are dropped.
 */
export interface Notice { text: string; wake: boolean; once?: string }

/**
 * A hook is an async module (default-export the function, `satisfies BeforeHook` / `AfterHook`) with the whole acyclic
 * API, or a shell command reading the same JSON on stdin and writing the result as JSON on stdout (exit 0 and no output:
 * allow / no changes; exit 2: deny, stderr the reason; any other failure: deny / blocked). It runs only while its
 * plugin is enabled, as the agent it runs for, which is charged its time and cost. Its own requests pass every
 * before-hook not already running on that request.
 */
export type HookEvent = "tool" | "model" | "spawn" | "fork" | "merge" | "commit";

/**
 * `hooks/before-<event>.{ts,sh}`, on each request of that event by you or anyone below you. Without rewrites all run on
 * the request and the strictest wins: deny, then ask, then allow. `input` replaces the whole input; rewrites apply one
 * at a time, highest issuer first, then every hook must allow the final request unchanged (a rewrite then is a deny).
 * Asks go out after that, about the final request. `to: "command"` runs the command named in the plugin's settings.
 */
export type BeforeHook<C = unknown> = (e: { request: Request; config: C }) => Decision | Promise<Decision>;

/**
 * `hooks/after-<n>.{ts,sh}`, on each change to the path its plugin.ts `after` names; across restarts, in order. The
 * changes it returns commit together with its position, so they apply exactly once (if a `check` fails, it runs again on
 * the same delivery). Anything else it does may repeat after a crash: make it idempotent with `key`, which is stable per
 * delivery. Under live/agents/self/ or .volumes/home/ it runs for each agent the plugin applies to; else once, for the
 * enabler. If it fails it stops there, blocked in live/agents/self/hooks/; fix it to continue.
 */
export type AfterHook<C = unknown> = (e: { delivery: Delivery; config: C; key: string }) => Change[] | Promise<Change[]>;

/** What a before-hook sees. Under a host (Claude Code, Codex, Pi) only spawn, fork, merge and commit reach before-hooks. */
export type Request =
  | { readonly event: "tool"; readonly from: string; readonly action: "exec"; readonly command: string; readonly tool: string; readonly input: object }
  | { readonly event: "tool"; readonly from: string; readonly action: "read" | "write" | "edit"; readonly path: Path; readonly tool: string; readonly input: object }
  | { readonly event: "tool"; readonly from: string; readonly action: "search" | "web" | "mcp" | "other"; readonly tool: string; readonly input: object }
  | { readonly event: "model"; readonly from: string; readonly model: string; readonly input: object }
  | { readonly event: "spawn" | "fork"; readonly from: string; readonly child: string; readonly input: object }
  /** `changes` is a folder of the changed files. */
  | { readonly event: "merge" | "commit"; readonly from: string; readonly changed: number; readonly changes: Path; readonly input: object };

export type Decision =
  | { allow: true }
  | { allow: true; input: object }
  | { allow: false; reason: string }
  | { ask: string; to: "person" | "parent" | "model" | "command" };

/* Provider plugins: plugins/<provider>/provider.ts --------------------- */

/** A tool definition exactly as trained (sys/native/<family>/). */
export interface NativeTool { readonly name: string; readonly definition: object }

/**
 * A settings file. `base.tools` is the trained set; models use it best unchanged. Any tool change re-sends your whole
 * context once at the next revision (priced in live/agents/self/context/).
 */
export interface ProviderProfile {
  tools: NativeTool[];
  cache: { breakpoints: ("header" | "segments")[]; ttl: "5m" | "1h" };
  compaction: "native-when-available" | "portable";
  /** Summary requests: drop tools the provider runs itself, or keep the header unchanged. */
  summaryHeader: "strip-executable-builtins" | "keep";
  effort: Record<"low" | "medium" | "high", object>;
  sampling: { temperature: number | null; topP: number | null; maxOutputTokens: number | null };
}
