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

// A failed task and a refused action (GraphCoder's ✗ status line) are
// failures unless the awaited condition names them; a refusal is named by any
// awaited text it contains.
const failures = visible => [...visible.matchAll(/task [0-9a-f-]{36} failed: ([^\n]*)|✗ ([^\n]*)/g)];
const expected = (failure, condition) => failure[1] !== undefined
  ? typeof condition?.task_failure === "string" && condition.task_failure.length > 0
    && failure[1].startsWith(condition.task_failure)
  : (condition?.contains ?? []).some(text => text.length > 0 && failure[2].includes(text));
export class FailureHistory {
  #handled = new Set();
  unexpected(visible, condition) {
    return failures(visible).find(failure => !expected(failure, condition)
      && !this.#handled.has(failure[0]))?.[0];
  }
  accept(visible, condition) {
    for (const failure of failures(visible)) if (expected(failure, condition)) this.#handled.add(failure[0]);
  }
}
