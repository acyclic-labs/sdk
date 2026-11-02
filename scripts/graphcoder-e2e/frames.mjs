// Observe completed terminal transactions and retain only explicitly handled failures.
export class TerminalFrames {
  #pending = 0;
  #synchronized = false;
  constructor(terminal) {
    this.terminal = terminal;
    const mode = enabled => parameters => {
      if (parameters.includes(2026)) this.#synchronized = enabled;
      return false;
    };
    this.handlers = [
      terminal.parser.registerCsiHandler({ prefix: "?", final: "h" }, mode(true)),
      terminal.parser.registerCsiHandler({ prefix: "?", final: "l" }, mode(false)),
    ];
  }
  ready() { return this.#pending === 0 && !this.#synchronized; }
  write(bytes, frame) {
    this.#pending++;
    return new Promise(resolve => this.terminal.write(bytes, () => {
      this.#pending--;
      if (this.ready()) frame();
      resolve();
    }));
  }
  dispose() { for (const handler of this.handlers) handler.dispose(); }
}

// Query errors are rendered in the status row, outside the task transcript.
export function queryFailure(visible) {
  const notice = visible.slice(visible.lastIndexOf("\n") + 1);
  return /^(?:storage failure:|Usage query ended:)/.test(notice) ? notice : undefined;
}
function* failures(visible) {
  for (const failure of visible.matchAll(/task [0-9a-f-]{36} failed: ([^\n]*)/g))
    yield { text: failure[0], reason: failure[1], kind: "task_failure" };
  const notice = queryFailure(visible);
  if (notice) yield { text: notice, reason: notice, kind: "query_failure" };
}
const expected = (failure, condition) => typeof condition?.[failure.kind] === "string"
  && condition[failure.kind].length > 0 && failure.reason.startsWith(condition[failure.kind]);
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
