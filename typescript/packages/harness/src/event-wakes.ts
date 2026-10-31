import { decodeMetadata, type EventStorage, type Placement, type StreamChange } from "./event-storage.js";

const HEADER = "index", FORMAT = "acyclic.host.wakes/v1";
interface Index { format: typeof FORMAT; actors: number; pending: number; first: string | null; last: string | null }
interface Registration {
  paths: string[]; generation: number; pending: boolean; previous: string | null; next: string | null;
}
interface Node { actor: string | null; first: string | null; next: string | null }
const nullable = (value: unknown): value is string | null => value === null || typeof value === "string";
const count = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const parent = (prefix: string) => prefix.slice(0, prefix.slice(0, -1).lastIndexOf("/") + 1);

function corrupt(): never {
  throw Object.assign(new Error("STEP actor-wake-read failed: invalid index; recovery: restore the original wake metadata from a verified backup"), { code: "data_loss" });
}

/** Durable membership in the original publication transaction. Exact changes
 * visit one path; subtree changes visit only that prefix's registered nodes.
 * Pending links survive owner loss without enumerating unrelated Actors. */
export class WakeIndex {
  constructor(private readonly storage: EventStorage, private readonly maximumActors: number) {
    if (!Number.isSafeInteger(maximumActors) || maximumActors <= 0 || maximumActors > 0xffff_ffff) {
      throw { code: "invalid_argument", step: "actor-wake-open", recovery: "supply the journal's positive u32 path capacity" };
    }
    storage.transaction(() => {
      const header = storage.get(HEADER);
      if (header !== undefined) { this.index(header); return; }
      // Unversioned metadata is refused, never silently reopened as an empty index.
      if (storage.entries().length !== 0) this.unsupported();
      this.saveIndex({ format: FORMAT, actors: 0, pending: 0, first: null, last: null });
      this.saveNode("", { actor: null, first: null, next: null });
    });
  }

  private unsupported(): never {
    throw Object.assign(new Error("STEP actor-wake-open failed: unsupported metadata version; recovery: open a new consistency domain with the current wake format"), { code: "failed_precondition" });
  }
  private index(value = this.storage.get(HEADER)): Index {
    if (value === undefined) return corrupt();
    const row = decodeMetadata(value) as Partial<Index> | null;
    if (typeof row !== "object" || row === null) return corrupt();
    if (row.format !== FORMAT) this.unsupported();
    if (!count(row.actors) || !count(row.pending) || row.pending > row.actors || !nullable(row.first) || !nullable(row.last)
      || (row.pending === 0) !== (row.first === null) || (row.pending === 0) !== (row.last === null)) return corrupt();
    return row as Index;
  }
  private saveIndex(row: Index): void { this.storage.put(HEADER, JSON.stringify(row)); }
  private registration(actor: string): Registration | undefined {
    const value = this.storage.get(`actor:${actor}`);
    if (value === undefined) return undefined;
    const row = decodeMetadata(value) as Partial<Registration> | null;
    if (typeof row !== "object" || row === null || !Array.isArray(row.paths) || row.paths.length !== 2
      || row.paths.some(path => typeof path !== "string" || !path || path.length > 4096)
      || row.paths[0] === row.paths[1] || !count(row.generation) || typeof row.pending !== "boolean"
      || !nullable(row.previous) || !nullable(row.next)
      || (!row.pending && (row.previous !== null || row.next !== null))) return corrupt();
    return row as Registration;
  }
  private required(actor: string): Registration { return this.registration(actor) ?? corrupt(); }
  private save(actor: string, row: Registration): void { this.storage.put(`actor:${actor}`, JSON.stringify(row)); }
  private node(prefix: string): Node | undefined {
    const value = this.storage.get(`path:${prefix}`);
    if (value === undefined) return undefined;
    const row = decodeMetadata(value) as Partial<Node> | null;
    if (typeof row !== "object" || row === null || !nullable(row.actor) || !nullable(row.first) || !nullable(row.next)
      || (row.first !== null && (parent(row.first) !== prefix || row.first.length <= prefix.length))
      || (row.next !== null && (parent(row.next) !== parent(prefix) || row.next === prefix))) return corrupt();
    return row as Node;
  }
  private saveNode(prefix: string, node: Node): void { this.storage.put(`path:${prefix}`, JSON.stringify(node)); }

  private insert(path: string, actor: string): void {
    let prefix = "";
    let node = this.node(prefix) ?? corrupt();
    for (const part of path.split("/")) {
      const next = `${prefix}${part}/`;
      let child = this.node(next);
      if (!child) {
        child = { actor: null, first: null, next: node.first };
        this.saveNode(next, child);
        this.saveNode(prefix, { ...node, first: next });
      }
      prefix = next;
      node = child;
    }
    this.saveNode(prefix, { ...node, actor });
  }

  async register(actor: string, paths: string[]): Promise<Placement> {
    if (!actor || new TextEncoder().encode(actor).length > 1024 || paths.length !== 2
      || paths[0] === paths[1] || paths.some(path => !path || path.length > 4096)) return { refused: "invalid_argument" };
    const result = this.storage.transaction<Placement>(() => {
      const index = this.index(), existing = this.registration(actor);
      if (existing) return existing.paths.every(path => paths.includes(path)) ? { admitted: true } : { refused: "conflict" };
      if (paths.some(path => this.node(`${path}/`)?.actor != null)) return { refused: "conflict" };
      if (index.actors >= this.maximumActors) return { refused: "capacity" };
      for (const path of paths) this.insert(path, actor);
      this.save(actor, { paths, generation: 0, pending: false, previous: null, next: null });
      this.saveIndex({ ...index, actors: index.actors + 1 });
      return { admitted: true };
    });
    if ("admitted" in result) await this.storage.flush();
    return result;
  }

  private select(prefix: string, subtree: boolean, actors: Set<string>, visited: Set<string>): void {
    const selected = this.node(prefix);
    if (!selected) return;
    if (!subtree) { if (selected.actor !== null) actors.add(selected.actor); return; }
    const stack = [{ prefix, node: selected }];
    while (stack.length !== 0) {
      const { prefix, node } = stack.pop()!;
      if (visited.has(prefix)) continue;
      visited.add(prefix);
      if (node.actor !== null) actors.add(node.actor);
      const siblings = new Set<string>();
      for (let child = node.first; child !== null;) {
        if (siblings.has(child)) return corrupt();
        siblings.add(child);
        const node = this.node(child) ?? corrupt();
        stack.push({ prefix: child, node });
        child = node.next;
      }
    }
  }

  /** Joins the original journal append transaction, including every index link. */
  changed(source: string, changes: readonly StreamChange[]): void {
    const actors = new Set<string>(), visited = new Set<string>();
    for (const [path, subtree] of changes) this.select(`${path}/`, subtree, actors, visited);
    actors.delete(source);
    if (actors.size === 0) return;
    const index = this.index();
    for (const actor of actors) {
      const row = this.required(actor);
      if (!Number.isSafeInteger(row.generation + 1)) {
        throw new Error("STEP actor-wake failed: generation exhausted; recovery: move actors to a new consistency domain");
      }
      if (!row.pending) {
        if (index.last !== null) {
          const last = this.required(index.last);
          this.save(index.last, { ...last, next: actor });
        } else index.first = actor;
        row.previous = index.last;
        index.last = actor;
        ++index.pending;
      }
      this.save(actor, { ...row, generation: row.generation + 1, pending: true });
    }
    this.saveIndex(index);
  }

  pendingCount(): number { return this.index().pending; }
  pending(): [string, number][] {
    const index = this.index(), result: [string, number][] = [];
    let previous: string | null = null;
    for (let actor = index.first; actor !== null;) {
      if (result.length >= index.pending) return corrupt();
      const row = this.required(actor);
      if (!row.pending || row.previous !== previous) return corrupt();
      result.push([actor, row.generation]);
      previous = actor;
      actor = row.next;
    }
    if (result.length !== index.pending || previous !== index.last) return corrupt();
    return result;
  }

  acknowledge(actor: string, generation: number): void {
    this.storage.transaction(() => this.acknowledgeInTransaction(actor, generation));
  }

  private acknowledgeInTransaction(actor: string, generation: number): void {
    const row = this.registration(actor);
    if (!row?.pending || row.generation !== generation) return;
    const index = this.index();
    if (index.pending === 0 || (row.previous === null && index.first !== actor)
      || (row.next === null && index.last !== actor)) return corrupt();
    if (row.previous === null) index.first = row.next;
    else {
      const previous = this.required(row.previous);
      if (!previous.pending || previous.next !== actor) return corrupt();
      this.save(row.previous, { ...previous, next: row.next });
    }
    if (row.next === null) index.last = row.previous;
    else {
      const next = this.required(row.next);
      if (!next.pending || next.previous !== actor) return corrupt();
      this.save(row.next, { ...next, previous: row.previous });
    }
    --index.pending;
    this.save(actor, { ...row, pending: false, previous: null, next: null });
    this.saveIndex(index);
  }
}
