// Observe completed terminal transactions and retain only explicitly handled failures.
export class TerminalFrames {
  #pending = 0;
  // Undefined until the producer first uses synchronized transactions.
  #synchronized;
  #boundary = false;
  #completed = false;
  #available = false;
  constructor(terminal, frame) {
    this.terminal = terminal;
    this.frame = frame;
    const mode = enabled => parameters => {
      if (parameters.includes(2026)) {
        const completed = this.#synchronized && !enabled;
        this.#synchronized = enabled;
        if (completed) {
          // A pipe chunk can contain several transactions. Observe this one
          // before parsing the next redraw, while these cells still exist.
          this.#completed = true;
          this.#boundary = true;
          this.#available = true;
          try { this.frame(); } finally { this.#boundary = false; }
        }
      }
      return false;
    };
    this.handlers = [
      terminal.parser.registerCsiHandler({ prefix: "?", final: "h" }, mode(true)),
      terminal.parser.registerCsiHandler({ prefix: "?", final: "l" }, mode(false)),
    ];
  }
  ready() { return (this.#pending === 0 || this.#boundary) && !this.#synchronized; }
  available() { return this.ready() && this.#available; }
  consume() { this.#available = false; }
  write(bytes) {
    this.#pending++;
    return new Promise(resolve => this.terminal.write(bytes, () => {
      this.#pending--;
      if (!this.#completed && this.ready()) {
        // Out-of-band observations still report errors but cannot admit input
        // against a consumed transaction once synchronized output begins.
        if (this.#synchronized === undefined) this.#available = true;
        this.frame();
      }
      this.#completed = false;
      resolve();
    }));
  }
  dispose() { for (const handler of this.handlers) handler.dispose(); }
}

// Query errors are rendered in the status row, outside the task transcript.
export function queryFailure(visible) {
  const notice = visible.slice(visible.lastIndexOf("\n") + 1).replace(/^✗ /, "");
  return /^(?:resource not found:|Filesystem query ended:|Usage query ended:)/.test(notice) ? notice : undefined;
}
const NATIVE_FAILURE = /(task [0-9a-f-]{36}) failed: ([^\n]*)|✗ ([^\n]*)/g;
function* failures(diagnostic, status = diagnostic) {
  // The status is authoritative even when the transcript's current logical
  // line is incomplete. Keep its complete visible row outside that cut.
  const row = status.slice(status.lastIndexOf("\n") + 1), notice = queryFailure(status);
  const first = row.trim().matchAll(NATIVE_FAILURE).next().value;
  const task = first?.index === 0 && first[1] !== undefined;
  const refusal = task ? undefined
    : /^(?:✗ )?(storage failure:.*|.* is not available in this view)$/.exec(row.trim())?.[1];
  const offset = diagnostic.endsWith(row) ? diagnostic.length - row.length : diagnostic.length;
  for (const failure of diagnostic.matchAll(NATIVE_FAILURE)) {
    if ((notice || refusal) && failure.index >= offset) continue;
    yield failure[1] !== undefined
      ? { text: failure[0], reason: failure[2], kind: "task_failure", identity: failure[1] }
      : { text: failure[0], reason: failure[3], kind: "action_refusal" };
  }
  if (notice || refusal) yield { text: row, reason: notice ?? refusal,
    kind: notice ? "query_failure" : "action_refusal" };
}
const expected = (failure, condition) => failure.kind === "action_refusal"
  ? (condition?.contains ?? []).some(text => text.length > 0 && failure.reason.includes(text))
  : typeof condition?.[failure.kind] === "string" && condition[failure.kind].length > 0
    && failure.reason.startsWith(condition[failure.kind]);
export function expectedTaskFailure(visible, condition) {
  for (const failure of failures(visible)) {
    if (failure.kind === "task_failure" && expected(failure, condition)) return true;
  }
  return false;
}
export class FailureHistory {
  #handled = new Map();
  unexpected(diagnostic, condition, assertion, status = diagnostic) {
    for (const failure of failures(diagnostic, status)) if (!expected(failure, condition) && !expected(failure, assertion)
      && !expected(failure, this.#handled.get(failure.identity ?? failure.text))) return failure.text;
  }
  accept(diagnostic, condition, assertion, status = diagnostic) {
    const handled = new Map();
    for (const failure of failures(diagnostic, status)) {
      const key = failure.identity ?? failure.text, previous = this.#handled.get(key);
      // The task and explicitly admitted prefix survive clipping/reflow. Other
      // notices retain exact text identity. Retain only this frame's failures.
      if (expected(failure, condition)) handled.set(key, condition);
      else if (expected(failure, assertion)) handled.set(key, assertion);
      else if (expected(failure, previous)) handled.set(key, previous);
    }
    this.#handled = handled;
  }
}
