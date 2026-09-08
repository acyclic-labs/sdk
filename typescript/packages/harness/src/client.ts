import type { AggregateKind, Authority, OperationId } from "./index.js";

export interface ReplayCursor {
  readonly generation: string;
  readonly revision: bigint;
}

export interface ClientEvent<Event = unknown> {
  readonly authority: Authority;
  readonly revision: bigint;
  readonly operationId: OperationId;
  readonly event: Event;
}

export interface Delivery<Event = unknown> {
  readonly authority: Authority;
  readonly generation: string;
  readonly fromRevision: bigint;
  readonly throughRevision: bigint;
  readonly events: readonly ClientEvent<Event>[];
  readonly live: boolean;
}

export interface ClientCommand<Payload = unknown> {
  readonly operationId: OperationId;
  readonly authority: Authority;
  readonly kind: string;
  readonly payload: Payload;
  readonly offlineSafe?: boolean;
}

export interface Connection<Event = unknown> extends AsyncIterable<Delivery<Event>> {
  send(command: ClientCommand): Promise<void>;
  close(): Promise<void> | void;
}

export interface Transport<Event = unknown> {
  connect(cursors: ReadonlyMap<string, ReplayCursor>, signal?: AbortSignal): Promise<Connection<Event>>;
}

export interface OutboxStore {
  load(): Promise<readonly ClientCommand[]>;
  put(command: ClientCommand): Promise<void>;
  delete(operationId: OperationId): Promise<void>;
}

export class MemoryOutbox implements OutboxStore {
  readonly #commands = new Map<OperationId, ClientCommand>();

  async load(): Promise<readonly ClientCommand[]> {
    return [...this.#commands.values()];
  }

  async put(command: ClientCommand): Promise<void> {
    this.#commands.set(command.operationId, command);
  }

  async delete(operationId: OperationId): Promise<void> {
    this.#commands.delete(operationId);
  }
}

export type ClientListener<Event> = (event: ClientEvent<Event>) => void;

/** Framework-neutral external store derived only from authoritative events. */
export class ProjectionStore<State, Event = unknown> {
  #state: State;
  readonly #listeners = new Set<() => void>();
  readonly #unsubscribe: () => void;

  constructor(client: HarnessClient<Event>, initial: State, reduce: (state: State, event: ClientEvent<Event>) => State) {
    this.#state = initial;
    this.#unsubscribe = client.subscribe(event => {
      this.#state = reduce(this.#state, event);
      for (const listener of this.#listeners) listener();
    });
  }

  /** React-compatible immutable snapshot accessor. */
  getSnapshot = (): State => this.#state;

  /** Svelte/React-compatible subscription. */
  subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  /** Detaches the store from its client. */
  dispose(): void {
    this.#unsubscribe();
    this.#listeners.clear();
  }
}

/** Framework-neutral reconnecting client with authoritative cursor validation and a safe outbox. */
export class HarnessClient<Event = unknown> {
  readonly #cursors = new Map<string, ReplayCursor>();
  readonly #listeners = new Set<ClientListener<Event>>();
  #connection: Connection<Event> | undefined;

  constructor(
    readonly transport: Transport<Event>,
    readonly outbox: OutboxStore = new MemoryOutbox(),
  ) {}

  handle(kind: AggregateKind, id: string): AggregateHandle<Event> {
    return new AggregateHandle(this, { kind, id });
  }

  agent(id: string): AgentHandle<Event> {
    return new AgentHandle(this, { kind: "agent", id });
  }

  conversation(id: string): ConversationHandle<Event> {
    return new ConversationHandle(this, { kind: "conversation", id });
  }

  session(id: string): SessionHandle<Event> {
    return new SessionHandle(this, { kind: "session", id });
  }

  turn(id: string): TurnHandle<Event> {
    return new TurnHandle(this, { kind: "turn", id });
  }

  task(id: string): TaskHandle<Event> {
    return new TaskHandle(this, { kind: "task", id });
  }

  /** Registers at-least-once projection delivery; callbacks must be idempotent. */
  subscribe(listener: ClientListener<Event>): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  async submit(command: ClientCommand): Promise<void> {
    const safelyReplayable = command.offlineSafe && command.kind !== "interaction.resolve.approval";
    if (this.#connection !== undefined) {
      if (safelyReplayable) await this.outbox.put(command);
      try {
        return await this.#connection.send(command);
      } catch (error) {
        if (safelyReplayable && error instanceof TerminalAdmissionError) {
          await this.outbox.delete(command.operationId);
        }
        throw error;
      }
    }
    if (!safelyReplayable) {
      throw new Error("command is not safe for the offline outbox");
    }
    await this.outbox.put(command);
  }

  /** Installs an authoritative snapshot cursor after synchronously resetting projections. */
  rebase(authority: Authority, cursor: ReplayCursor, resetProjections: () => void): void {
    if (cursor.generation === "" || cursor.revision < 0n) throw new RangeError("invalid cursor");
    resetProjections();
    this.#cursors.set(authorityKey(authority), cursor);
  }

  /** Runs reconnect/replay until aborted; transport failures use bounded exponential backoff. */
  async run(signal?: AbortSignal): Promise<void> {
    let delay = 50;
    while (!signal?.aborted) {
      try {
        const connection = await this.transport.connect(this.#cursors, signal);
        this.#connection = connection;
        await this.#flushOutbox(connection);
        for await (const delivery of connection) await this.#accept(delivery);
        delay = 50;
      } catch (error) {
        if (signal?.aborted) break;
        if (error instanceof ReplayError) this.#cursors.delete(authorityKey(error.authority));
        await abortableDelay(delay, signal);
        delay = Math.min(delay * 2, 5_000);
      } finally {
        const connection = this.#connection;
        this.#connection = undefined;
        await connection?.close();
      }
    }
  }

  async #flushOutbox(connection: Connection<Event>): Promise<void> {
    for (const command of await this.outbox.load()) {
      try {
        await connection.send(command);
      } catch (error) {
        if (error instanceof TerminalAdmissionError) {
          await this.outbox.delete(command.operationId);
          continue;
        }
        throw error;
      }
    }
  }

  async #accept(delivery: Delivery<Event>): Promise<void> {
    const key = authorityKey(delivery.authority);
    const previous = this.#cursors.get(key);
    const expected = previous?.revision ?? 0n;
    if (previous !== undefined && previous.generation !== delivery.generation) {
      throw new ReplayError(delivery.authority, "replay generation changed");
    }
    if (delivery.fromRevision !== expected || delivery.throughRevision < delivery.fromRevision) {
      throw new ReplayError(delivery.authority, "non-contiguous delivery");
    }
    let revision = expected;
    for (const event of delivery.events) {
      revision += 1n;
      if (authorityKey(event.authority) !== key || event.revision !== revision) {
        throw new ReplayError(delivery.authority, "event authority or revision mismatch");
      }
    }
    if (revision !== delivery.throughRevision) {
      throw new ReplayError(delivery.authority, "delivery coverage mismatch");
    }
    let committed = expected;
    for (const event of delivery.events) {
      for (const listener of this.#listeners) listener(event);
      await this.outbox.delete(event.operationId);
      committed = event.revision;
      this.#cursors.set(key, { generation: delivery.generation, revision: committed });
    }
    if (delivery.events.length === 0) {
      this.#cursors.set(key, { generation: delivery.generation, revision });
    }
  }
}

export class AggregateHandle<Event = unknown> {
  constructor(
    readonly client: HarnessClient<Event>,
    readonly authority: Authority,
  ) {}

  submit<Payload>(command: Omit<ClientCommand<Payload>, "authority">): Promise<void> {
    return this.client.submit({ ...command, authority: this.authority });
  }
}

export class AgentHandle<Event = unknown> extends AggregateHandle<Event> {}
export class ConversationHandle<Event = unknown> extends AggregateHandle<Event> {}
export class SessionHandle<Event = unknown> extends AggregateHandle<Event> {}
export class TurnHandle<Event = unknown> extends AggregateHandle<Event> {}
export class TaskHandle<Event = unknown> extends AggregateHandle<Event> {}

export class ReplayError extends Error {
  constructor(
    readonly authority: Authority,
    message: string,
  ) {
    super(message);
  }
}

/** A command was authoritatively rejected and must never remain retryable. */
export class TerminalAdmissionError extends Error {}

function authorityKey(authority: Authority): string {
  return `${authority.kind}:${authority.id}`;
}

async function abortableDelay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(resolve, milliseconds);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timeout);
        reject(signal.reason);
      },
      { once: true },
    );
  });
}
