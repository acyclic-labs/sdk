import { createInterface, type Interface } from "node:readline/promises";
import { stdin, stdout } from "node:process";
import { GraphCoderError, GraphCoderUi, sessionId, agentId, approvalId, type GraphCoderTransport, type GraphCoderUiState } from "./api.js";
import { createMockTransport, MOCK_FIXTURES, type MockFixture } from "./mock.js";

export interface TerminalIO {
  readonly input?: NodeJS.ReadableStream;
  readonly output?: NodeJS.WritableStream;
}

export interface TerminalOptions {
  readonly fixture?: MockFixture;
  readonly mode?: "interactive" | "headless";
  readonly io?: TerminalIO;
}

function writeLine(io: TerminalIO, value: unknown): void {
  const output = io.output ?? stdout;
  output.write(`${JSON.stringify(value, (_key, item) => {
    if (typeof item === "bigint") return String(item);
    if (item instanceof Uint8Array) return Array.from(item);
    return item;
  })}\n`);
}

function errorProjection(error: unknown): Record<string, unknown> {
  if (error instanceof GraphCoderError) return { code: error.code, message: error.message };
  return { code: "transport", message: error instanceof Error ? error.message : String(error) };
}

function words(line: string): string[] {
  const values = line.trim().split(/\s+/u);
  return values.length === 1 && values[0] === "" ? [] : values;
}

function stateProjection(state: GraphCoderUiState): Record<string, unknown> {
  return {
    sessions: state.sessions,
    selectedSession: state.selectedSession?.summary,
    agents: state.selectedSession?.agents,
    activity: state.activity,
    messages: state.messages,
    approvals: state.approvals,
    approvalsNext: state.approvalsNext,
    changes: state.changes,
    changesGeneration: state.changesGeneration,
    changeBody: state.changeBody,
    fileBody: state.fileBody,
    writeback: state.writeback,
  };
}

/**
 * Presentation adapter for the generic UI state machine. Native applications
 * can consume GraphCoderUi directly; this class only turns commands into
 * newline-delimited terminal output.
 */
export class GraphCoderTerminal {
  readonly #ui: GraphCoderUi;
  readonly #io: TerminalIO;

  constructor(transport: GraphCoderTransport, io: TerminalIO = {}) {
    this.#ui = new GraphCoderUi(transport);
    this.#io = io;
  }

  ui(): GraphCoderUi { return this.#ui; }

  async command(line: string): Promise<void> {
    const parts = words(line);
    const command = parts[0];
    if (command === undefined || command === "") return;
    if (command === "help") {
      writeLine(this.#io, { ok: true, commands: ["list [cursor]", "start <operation> <prompt>", "open <id>", "resume <id>", "activity [cursor]", "messages [cursor]", "approvals [cursor]", "approve <id> <yes|no>", "message <sender> <recipient> <body>", "cancel", "changes", "diff <path>", "file <path>", "writeback <operation> <generation> <yes|no>", "quit"] });
      return;
    }
    if (command === "quit" || command === "exit") {
      writeLine(this.#io, { ok: true, exited: true });
      return;
    }
    if (command === "list") {
      await this.#ui.dispatch({ kind: "list_sessions", ...(parts[1] === undefined ? {} : { after: parts[1] }) });
      writeLine(this.#io, { ok: true, value: this.#ui.state().sessions, next: this.#ui.state().sessionsNext });
      return;
    }
    if (command === "start") {
      const operationId = parts[1];
      const prompt = parts.slice(2).join(" ");
      if (operationId === undefined || prompt.trim() === "") throw new GraphCoderError("invalid_input", "start requires <operation> <prompt>");
      await this.#ui.dispatch({ kind: "start_session", operationId, prompt });
      writeLine(this.#io, { ok: true, value: stateProjection(this.#ui.state()) });
      return;
    }
    if (command === "open" || command === "resume") {
      const id = parts[1];
      if (id === undefined) throw new GraphCoderError("invalid_input", `${command} requires a session id`);
      await this.#ui.dispatch(command === "open" ? { kind: "open_session", sessionId: sessionId(id) } : { kind: "resume_session", sessionId: sessionId(id) });
      writeLine(this.#io, { ok: true, value: stateProjection(this.#ui.state()) });
      return;
    }
    if (command === "activity" || command === "messages" || command === "approvals") {
      const after = parts[1];
      const page = after === undefined ? {} : { after };
      await this.#ui.dispatch(command === "activity" ? { kind: "load_activity", ...page } : command === "messages" ? { kind: "load_messages", ...page } : { kind: "load_approvals", ...page });
      const state = this.#ui.state();
      writeLine(this.#io, { ok: true, value: command === "activity" ? state.activity : command === "messages" ? state.messages : state.approvals, next: command === "activity" ? state.activityNext : command === "messages" ? state.messagesNext : state.approvalsNext });
      return;
    }
    if (command === "approve") {
      const id = parts[1];
      const value = parts[2];
      if (id === undefined || (value !== "yes" && value !== "no")) throw new GraphCoderError("invalid_input", "approve requires <id> <yes|no>");
      await this.#ui.dispatch({ kind: "resolve_approval", approvalId: approvalId(id), approved: value === "yes" });
      writeLine(this.#io, { ok: true, value: this.#ui.state().approvals });
      return;
    }
    if (command === "message") {
      const sender = parts[1];
      const recipient = parts[2];
      const body = line.trim().split(/\s+/u).slice(3).join(" ");
      if (sender === undefined || recipient === undefined || body === "") throw new GraphCoderError("invalid_input", "message requires <sender> <recipient> <body>");
      await this.#ui.dispatch({ kind: "send_message", senderId: agentId(sender), recipientId: agentId(recipient), body });
      writeLine(this.#io, { ok: true, value: this.#ui.state().messages.at(-1) });
      return;
    }
    if (command === "cancel") {
      await this.#ui.dispatch({ kind: "cancel_session" });
      writeLine(this.#io, { ok: true, value: this.#ui.state().selectedSession?.summary });
      return;
    }
    if (command === "changes") {
      await this.#ui.dispatch({ kind: "list_changes" });
      writeLine(this.#io, { ok: true, value: { generation: this.#ui.state().changesGeneration, items: this.#ui.state().changes } });
      return;
    }
    if (command === "diff") {
      const path = parts[1];
      if (path === undefined) throw new GraphCoderError("invalid_input", "diff requires a path");
      await this.#ui.dispatch({ kind: "read_change", path });
      writeLine(this.#io, { ok: true, value: this.#ui.state().changeBody });
      return;
    }
    if (command === "file") {
      const path = parts[1];
      if (path === undefined) throw new GraphCoderError("invalid_input", "file requires a path");
      await this.#ui.dispatch({ kind: "read_file", path });
      writeLine(this.#io, { ok: true, value: this.#ui.state().fileBody });
      return;
    }
    if (command === "writeback") {
      const operationId = parts[1];
      const generation = parts[2];
      const approved = parts[3];
      if (operationId === undefined || generation === undefined || (approved !== "yes" && approved !== "no") || !/^\d+$/u.test(generation)) {
        throw new GraphCoderError("invalid_input", "writeback requires <operation> <generation> <yes|no>");
      }
      await this.#ui.dispatch({ kind: "approve_writeback", operationId, expectedGeneration: BigInt(generation), approved: approved === "yes" });
      writeLine(this.#io, { ok: true, value: this.#ui.state().writeback });
      return;
    }
    throw new GraphCoderError("invalid_input", `unknown command ${command}`);
  }

  async headless(commands: readonly string[]): Promise<number> {
    let failed = false;
    for (const line of commands) {
      try { await this.command(line); }
      catch (error) {
        failed = true;
        writeLine(this.#io, { ok: false, error: errorProjection(error) });
      }
    }
    return failed ? 1 : 0;
  }

  async interactive(): Promise<void> {
    const input = this.#io.input ?? stdin;
    const output = this.#io.output ?? stdout;
    const terminal = Boolean((input as NodeJS.ReadStream).isTTY && (output as NodeJS.WriteStream).isTTY);
    const readline: Interface = createInterface({
      input,
      output,
      terminal,
    });
    const pending = new Set<Promise<void>>();
    let closing = false;
    let resolveClosed!: () => void;
    const closed = new Promise<void>(resolve => { resolveClosed = resolve; });
    readline.once("close", resolveClosed);
    const runLine = async (line: string): Promise<void> => {
      if (closing) return;
      const trimmed = line.trim();
      if (trimmed === "quit" || trimmed === "exit") {
        closing = true;
        try { await this.command(line); }
        catch (error) { writeLine(this.#io, { ok: false, error: errorProjection(error) }); }
        readline.close();
        return;
      }
      try { await this.command(line); }
      catch (error) { writeLine(this.#io, { ok: false, error: errorProjection(error) }); }
    };
    readline.on("line", line => {
      const task = runLine(line);
      pending.add(task);
      void task.finally(() => pending.delete(task));
      // Keep accepting a second command while the first one waits on the
      // durable owner. This is what lets `cancel` interrupt a slow history
      // or model request in the interactive terminal.
      if (terminal && !closing) readline.prompt();
    });
    if (terminal) {
      readline.setPrompt("graphcoder> ");
      readline.prompt();
    }
    try {
      await closed;
      await Promise.all(pending);
    } finally {
      closing = true;
      readline.close();
      await Promise.allSettled(pending);
    }
  }
}

/**
 * Runs the public terminal command loop against a host-owned transport. The
 * executable, process bridge, and durable Harness remain the host's concern;
 * this helper only selects headless versus interactive presentation.
 */
export async function runCliWithTransport(argv: readonly string[], transport: GraphCoderTransport, io: TerminalIO = {}): Promise<number> {
  if (argv.some(value => value.startsWith("--fixture="))) {
    writeLine(io, { ok: false, error: "--fixture is only supported by the explicit mock CLI" });
    return 2;
  }
  const terminal = new GraphCoderTerminal(transport, io);
  if (argv.length > 0) return await terminal.headless(argv);
  if (io.input !== undefined || process.stdin.isTTY) {
    await terminal.interactive();
    return 0;
  }
  writeLine(io, { ok: true, usage: "graphcoder [command ...]" });
  return 0;
}

export async function runCli(argv: readonly string[], io: TerminalIO = {}): Promise<number> {
  const fixtureArgument = argv.find(value => value.startsWith("--fixture="));
  const fixture = fixtureArgument?.slice("--fixture=".length);
  if (fixture === undefined || !MOCK_FIXTURES.includes(fixture as MockFixture)) {
    writeLine(io, { ok: false, error: "an explicit supported --fixture is required", fixtures: MOCK_FIXTURES });
    return 2;
  }
  const transport = createMockTransport({ fixture: fixture as MockFixture });
  const commands = argv.filter(value => !value.startsWith("--fixture="));
  if (commands.length > 0) return await runCliWithTransport(commands, transport, io);
  if (io.input !== undefined || process.stdin.isTTY) {
    await new GraphCoderTerminal(transport, io).interactive();
    return 0;
  }
  writeLine(io, { ok: true, usage: "graphcoder --fixture=deterministic [command ...]" });
  return 0;
}

if (import.meta.main) process.exitCode = await runCli(process.argv.slice(2));
