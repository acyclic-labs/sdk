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
  output.write(`${JSON.stringify(value, (_key, item) => typeof item === "bigint" ? `${item}n` : item)}\n`);
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
      writeLine(this.#io, { ok: true, commands: ["list", "start <prompt>", "open <id>", "resume <id>", "activity", "messages", "approvals", "approve <id> <yes|no>", "message <sender> <recipient> <body>", "cancel", "changes", "diff <path>", "writeback <operation> <generation> <yes|no>", "quit"] });
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
      const prompt = line.slice(line.indexOf(command) + command.length).trim();
      await this.#ui.dispatch({ kind: "start_session", prompt });
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

  async headless(commands: readonly string[]): Promise<void> {
    for (const line of commands) {
      try { await this.command(line); }
      catch (error) { writeLine(this.#io, { ok: false, error: error instanceof Error ? error.message : String(error) }); }
    }
  }

  async interactive(): Promise<void> {
    const input = this.#io.input ?? stdin;
    const output = this.#io.output ?? stdout;
    const readline: Interface = createInterface({ input, output, terminal: true });
    try {
      for (;;) {
        const line = await readline.question("graphcoder> ");
        if (line.trim() === "quit" || line.trim() === "exit") { await this.command(line); return; }
        try { await this.command(line); }
        catch (error) { writeLine(this.#io, { ok: false, error: error instanceof Error ? error.message : String(error) }); }
      }
    } finally { readline.close(); }
  }
}

export async function runCli(argv: readonly string[], io: TerminalIO = {}): Promise<number> {
  const fixtureArgument = argv.find(value => value.startsWith("--fixture="));
  const fixture = fixtureArgument?.slice("--fixture=".length);
  if (fixture === undefined || !MOCK_FIXTURES.includes(fixture as MockFixture)) {
    writeLine(io, { ok: false, error: "an explicit supported --fixture is required", fixtures: MOCK_FIXTURES });
    return 2;
  }
  const transport = createMockTransport({ fixture: fixture as MockFixture });
  const terminal = new GraphCoderTerminal(transport, io);
  const commands = argv.filter(value => !value.startsWith("--fixture="));
  if (commands.length > 0) { await terminal.headless(commands); return 0; }
  if (io.input !== undefined || io.output !== undefined || process.stdin.isTTY) { await terminal.interactive(); return 0; }
  writeLine(io, { ok: true, usage: "graphcoder --fixture=deterministic [command ...]" });
  return 0;
}

if (import.meta.main) process.exitCode = await runCli(process.argv.slice(2));
