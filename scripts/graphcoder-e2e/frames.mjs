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
        // Once transactions are in use, out-of-band bytes still report errors
        // but cannot admit input against the preceding transaction's cells.
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
  return /^(?:storage failure:|Usage query ended:)/.test(notice) ? notice : undefined;
}
function* failures(visible) {
  const notice = queryFailure(visible), status = visible.lastIndexOf("\n") + 1;
  for (const failure of visible.matchAll(/(task [0-9a-f-]{36}) failed: ([^\n]*)|✗ ([^\n]*)/g)) {
    if (notice && failure.index >= status) continue;
    yield failure[1] !== undefined
      ? { text: failure[0], reason: failure[2], kind: "task_failure", identity: failure[1] }
      : { text: failure[0], reason: failure[3], kind: "action_refusal" };
  }
  if (notice) yield { text: visible.slice(status), reason: notice, kind: "query_failure" };
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
  unexpected(visible, condition) {
    for (const failure of failures(visible)) if (!expected(failure, condition)
      && !expected(failure, this.#handled.get(failure.identity ?? failure.text))) return failure.text;
  }
  accept(visible, condition) {
    const handled = new Map();
    for (const failure of failures(visible)) {
      const key = failure.identity ?? failure.text, previous = this.#handled.get(key);
      // The task and explicitly admitted prefix survive clipping/reflow. Other
      // notices retain exact text identity. Retain only this frame's failures.
      if (expected(failure, condition)) handled.set(key, condition);
      else if (expected(failure, previous)) handled.set(key, previous);
    }
    this.#handled = handled;
  }
}
