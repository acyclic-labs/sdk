/**
 * The agent tree and history explorer: a thin browser driver over the one
 * Rust reader (`WasmExplorer`). Rows are renders of committed records; the
 * page persists the reader's cache in IndexedDB so a reload renders at once.
 */
import { WasmExplorer, type WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";

/** One visible tree row. `latest` is the newest committed conversation record. */
export type ExplorerRow =
  | {
    kind: "agent"; path: string; depth: bigint; status: unknown | null;
    conversation: bigint | null; latest: Uint8Array | null; expanded: boolean;
  }
  | { kind: "loading"; depth: bigint };

/** One history row; `value` is null while it loads. */
export interface ExplorerHistoryRow { sequence: bigint; value: Uint8Array | null }

/** Connectivity and residency; `stale` marks an offline, cache-only view. */
export interface ExplorerHealth {
  /** Trusted source custody, independently of transport connectivity. */
  credentials: "unqualified" | "pending" | "current" | "unavailable" | "unsupported" | "sourceLost";
  /** One encoded snapshot waiting for source admission in the shared reader. */
  snapshotBytes: bigint;
  connectivity: "online" | "degraded" | "offline";
  retryAt: bigint | null;
  cacheBytes: bigint;
  /** Uncached charge, including Rust row views retained after eviction. */
  passedBytes: bigint;
  /** Unique Rust payload capacity; excludes allocator and JavaScript copies. */
  backingBytes: bigint;
  requests: bigint;
  follows: bigint;
  /** Milliseconds until the reader's next deadline (a read timeout or retry). */
  wakeIn: bigint | null;
  /** Discontinuities that must remain visible independently of connectivity. */
  issues: { stream: string; kind: "accessDenied" | "retentionLost" | "hierarchyChanged" }[];
}

export interface ExplorerOptions {
  /** Called once per animation frame after committed records changed. */
  render(rows: ExplorerRow[], explorer: AgentExplorer): void;
  /** IndexedDB key for the persisted cache; omit to keep it in memory only. */
  persist?: string;
  /** Device memory budget in bytes; defaults to `navigator.deviceMemory`. */
  memory?: number;
}

const DATABASE = "acyclic-reader";
const STORE = "snapshots";

function database(): Promise<IDBDatabase | undefined> {
  if (typeof indexedDB === "undefined") return Promise.resolve(undefined);
  return new Promise(resolve => {
    const request = indexedDB.open(DATABASE, 1);
    request.onupgradeneeded = () => request.result.createObjectStore(STORE);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(undefined);
  });
}

function load(db: IDBDatabase, key: string): Promise<Uint8Array | undefined> {
  return new Promise(resolve => {
    const request = db.transaction(STORE).objectStore(STORE).get(key);
    request.onsuccess = () => resolve(request.result instanceof Uint8Array ? request.result : undefined);
    request.onerror = () => resolve(undefined);
  });
}

/** Explorer over one stream session: one scheduler, one timer, one cache. */
export class AgentExplorer {
  readonly #core: WasmExplorer;
  readonly #options: ExplorerOptions;
  readonly #db: IDBDatabase | undefined;
  #frame: number | undefined;
  #fallbackFrame: ReturnType<typeof setTimeout> | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;
  readonly #render = () => {
    this.#frame = undefined;
    this.#fallbackFrame = undefined;
    if (this.#closed) return;
    this.#options.render(this.#core.frame() as ExplorerRow[], this);
    if (!this.#closed) this.#arm();
  };
  readonly #online = () => { this.#core.networkChanged(true); this.schedule(); };
  readonly #offline = () => { this.#core.networkChanged(false); this.schedule(); };
  readonly #hidden = () => { if (document.visibilityState === "hidden") this.save(); };

  private constructor(core: WasmExplorer, options: ExplorerOptions, db: IDBDatabase | undefined) {
    this.#core = core;
    this.#options = options;
    this.#db = db;
    globalThis.addEventListener?.("online", this.#online);
    globalThis.addEventListener?.("offline", this.#offline);
    globalThis.document?.addEventListener("visibilitychange", this.#hidden);
  }

  /** Opens the explorer, restoring the persisted cache before the first frame. */
  static async open(stream: WasmStream, options: ExplorerOptions): Promise<AgentExplorer> {
    const device = (globalThis.navigator as { deviceMemory?: number } | undefined)?.deviceMemory;
    const memory = options.memory ?? (device === undefined ? 0 : device * 2 ** 30);
    let explorer: AgentExplorer | undefined;
    const core = new WasmExplorer(stream, () => explorer?.schedule(), memory);
    const db = options.persist === undefined ? undefined : await database();
    if (db !== undefined && options.persist !== undefined) {
      const snapshot = await load(db, options.persist);
      if (snapshot !== undefined) {
        try { core.restore(snapshot); } catch { /* an unreadable snapshot is an empty cache */ }
      }
    }
    explorer = new AgentExplorer(core, options, db);
    if (globalThis.navigator?.onLine === false) core.networkChanged(false);
    explorer.schedule();
    return explorer;
  }

  /** Renders on the next animation frame; repeated calls coalesce. */
  schedule(): void {
    if (this.#closed || this.#frame !== undefined || this.#fallbackFrame !== undefined) return;
    if (typeof globalThis.requestAnimationFrame === "function") {
      this.#frame = globalThis.requestAnimationFrame(this.#render);
    } else {
      this.#fallbackFrame = setTimeout(this.#render, 16);
    }
  }

  /** The reader's one deadline becomes the page's one timer. */
  #arm(): void {
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    const wakeIn = this.health().wakeIn;
    if (wakeIn === null) return;
    this.#timer = setTimeout(() => { this.#timer = undefined; this.schedule(); }, Number(wakeIn));
  }

  history(): ExplorerHistoryRow[] { return this.#core.history() as ExplorerHistoryRow[]; }
  health(): ExplorerHealth { return this.#core.health() as ExplorerHealth; }
  setViewport(height: number, overscan: number, frameMs = 16): void { this.#core.setViewport(height, overscan, frameMs); this.schedule(); }
  scroll(rows: number): void { this.#core.scroll(rows); this.schedule(); }
  hover(path: string | null): void { this.#core.hover(path ?? undefined); this.schedule(); }
  focus(path: string | null): void { this.#core.focus(path ?? undefined); this.schedule(); }
  expand(path: string): void { this.#core.expand(path); this.schedule(); }
  collapse(path: string): void { this.#core.collapse(path); this.schedule(); }
  open(path: string, rows: number): void { this.#core.open(path, rows); this.schedule(); }
  closeHistory(): void { this.#core.close(); this.schedule(); }
  seek(sequence: bigint): void { this.#core.seek(sequence); this.schedule(); }
  jumpEnd(): void { this.#core.jumpEnd(); this.schedule(); }

  /** Persists admitted state; pending source admission preserves the saved cache. */
  save(): void {
    if (!this.#closed) this.#persist();
  }

  #persist(): void {
    if (this.#db === undefined || this.#options.persist === undefined) return;
    const snapshot = this.#core.snapshot();
    if (snapshot === undefined) return;
    const store = this.#db.transaction(STORE, "readwrite").objectStore(STORE);
    if (snapshot.length === 0) store.delete(this.#options.persist);
    else store.put(snapshot, this.#options.persist);
  }

  /** Persists, then drops every follow and read. */
  dispose(): void {
    if (this.#closed) return;
    this.#closed = true;
    try {
      this.#persist();
    } finally {
      globalThis.removeEventListener?.("online", this.#online);
      globalThis.removeEventListener?.("offline", this.#offline);
      globalThis.document?.removeEventListener("visibilitychange", this.#hidden);
      if (this.#timer !== undefined) clearTimeout(this.#timer);
      if (this.#frame !== undefined) globalThis.cancelAnimationFrame(this.#frame);
      if (this.#fallbackFrame !== undefined) clearTimeout(this.#fallbackFrame);
      this.#timer = undefined;
      this.#frame = undefined;
      this.#fallbackFrame = undefined;
      this.#core.free();
    }
  }
}
