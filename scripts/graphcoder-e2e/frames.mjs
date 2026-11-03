// Observe completed terminal transactions and retain only explicitly handled failures.
export class TerminalFrames {
  #pending = 0;
  #synchronized = false;
  #boundary = false;
  #completed = false;
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
  write(bytes) {
    this.#pending++;
    return new Promise(resolve => this.terminal.write(bytes, () => {
      this.#pending--;
      if (!this.#completed && this.ready()) this.frame();
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
  for (const failure of visible.matchAll(/task [0-9a-f-]{36} failed: ([^\n]*)|✗ ([^\n]*)/g)) {
    if (notice && failure.index >= status) continue;
    yield failure[1] !== undefined
      ? { text: failure[0], reason: failure[1], kind: "task_failure" }
      : { text: failure[0], reason: failure[2], kind: "action_refusal" };
  }
  if (notice) yield { text: visible.slice(status), reason: notice, kind: "query_failure" };
}
const expected = (failure, condition) => failure.kind === "action_refusal"
  ? (condition?.contains ?? []).some(text => text.length > 0 && failure.reason.includes(text))
  : typeof condition?.[failure.kind] === "string" && condition[failure.kind].length > 0
    && failure.reason.startsWith(condition[failure.kind]);
export class FailureHistory {
  #handled = new Set();
  unexpected(visible, condition) {
    for (const failure of failures(visible)) if (!expected(failure, condition)
      && !this.#handled.has(failure.text)) return failure.text;
  }
  accept(visible, condition) {
    for (const failure of failures(visible)) if (expected(failure, condition)) this.#handled.add(failure.text);
  }
}
