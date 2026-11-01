import { decodeMetadata, type EventStorage, type Placement, type StreamChange } from "./event-storage.js";

const HEADER = "index", FORMAT = "acyclic.host.wakes/v2";
interface Index { format: typeof FORMAT; actors: number; pending: number; first: string | null; last: string | null }
interface Registration {
  pathCount: number; revision: string; generation: number; pending: boolean; previous: string | null; next: string | null;
}
interface Node { owner: string | null; first: string | null; previous: string | null; next: string | null }
interface Member { previous: string | null; next: string | null }
const revision = (value: unknown): value is string => typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value);
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
  private durable: Promise<void> = Promise.resolve();
  constructor(private readonly storage: EventStorage, private readonly maximumActors?: number) {
    if (maximumActors !== undefined && (!Number.isSafeInteger(maximumActors) || maximumActors <= 0)) {
      throw { code: "invalid_argument", step: "actor-wake-open", recovery: "omit capacity or supply a positive safe integer" };
    }
    storage.transaction(() => {
      const header = storage.get(HEADER);
      if (header !== undefined) { this.index(header); return; }
      // Unversioned metadata is refused, never silently reopened as an empty index.
      if (storage.entries("", "", 1).length !== 0) this.unsupported();
      this.saveIndex({ format: FORMAT, actors: 0, pending: 0, first: null, last: null });
      this.saveNode("", { owner: null, first: null, previous: null, next: null });
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
    if (typeof row !== "object" || row === null || !count(row.pathCount)
      || !revision(row.revision) || !count(row.generation) || typeof row.pending !== "boolean"
      || !nullable(row.previous) || !nullable(row.next)
      || (!row.pending && (row.previous !== null || row.next !== null))) return corrupt();
    return row as Registration;
  }
  private paths(actor: string, count: number): string[] {
    const value = this.storage.get(`paths:${actor}`);
    if (value === undefined) return corrupt();
    const paths: unknown = decodeMetadata(value);
    if (!Array.isArray(paths) || paths.length !== count
      || paths.some(path => typeof path !== "string" || !path) || new Set(paths).size !== count) return corrupt();
    return paths;
  }
  private required(actor: string): Registration { return this.registration(actor) ?? corrupt(); }
  private save(actor: string, row: Registration): void { this.storage.put(`actor:${actor}`, JSON.stringify(row)); }
  private node(prefix: string): Node | undefined {
    const value = this.storage.get(`path:${prefix}`);
    if (value === undefined) return undefined;
    const row = decodeMetadata(value) as Partial<Node> | null;
    if (typeof row !== "object" || row === null || !nullable(row.owner) || !nullable(row.first) || !nullable(row.previous) || !nullable(row.next)
      || (row.first !== null && (parent(row.first) !== prefix || row.first.length <= prefix.length))
      || (row.next !== null && (parent(row.next) !== parent(prefix) || row.next === prefix))
      || (row.previous !== null && (parent(row.previous) !== parent(prefix) || row.previous === prefix))) return corrupt();
    return row as Node;
  }
  private saveNode(prefix: string, node: Node): void { this.storage.put(`path:${prefix}`, JSON.stringify(node)); }

  private memberKey(path: string, actor: string): string { return `member:${JSON.stringify([path, actor])}`; }
  private member(path: string, actor: string): Member {
    const value = this.storage.get(this.memberKey(path, actor));
    if (value === undefined) return corrupt();
    const row = decodeMetadata(value) as Partial<Member> | null;
    if (typeof row !== "object" || row === null || !nullable(row.previous) || !nullable(row.next)
      || row.previous === actor || row.next === actor) return corrupt();
    return row as Member;
  }
  private saveMember(path: string, actor: string, row: Member): void {
    this.storage.put(this.memberKey(path, actor), JSON.stringify(row));
  }
  private insert(path: string, actor: string): void {
    let prefix = "";
    let node = this.node(prefix) ?? corrupt();
    for (const part of path.split("/")) {
      const next = `${prefix}${part}/`;
      let child = this.node(next);
      if (!child) {
        child = { owner: null, first: null, previous: null, next: node.first };
        if (node.first !== null) {
          const sibling = this.node(node.first) ?? corrupt();
          this.saveNode(node.first, { ...sibling, previous: next });
        }
        this.saveNode(next, child);
        this.saveNode(prefix, { ...node, first: next });
      }
      prefix = next;
      node = child;
    }
    if (node.owner !== null) {
      const first = this.member(path, node.owner);
      this.saveMember(path, node.owner, { ...first, previous: actor });
    }
    this.saveMember(path, actor, { previous: null, next: node.owner });
    this.saveNode(prefix, { ...node, owner: actor });
  }

  private remove(path: string, actor: string): void {
    let prefix = `${path}/`, node = this.node(prefix) ?? corrupt();
    const member = this.member(path, actor);
    if (member.previous === null) {
      if (node.owner !== actor) return corrupt();
      node = { ...node, owner: member.next };
      this.saveNode(prefix, node);
    } else {
      const previous = this.member(path, member.previous);
      if (previous.next !== actor) return corrupt();
      this.saveMember(path, member.previous, { ...previous, next: member.next });
    }
    if (member.next !== null) {
      const next = this.member(path, member.next);
      if (next.previous !== actor) return corrupt();
      this.saveMember(path, member.next, { ...next, previous: member.previous });
    }
    this.storage.delete(this.memberKey(path, actor));
    // Retire empty prefixes at the same membership boundary; history cannot
    // accumulate dead nodes or add work to later subtree publications.
    while (prefix !== "" && node.owner === null && node.first === null) {
      const up = parent(prefix), ancestor = this.node(up) ?? corrupt();
      if (node.previous === null) {
        if (ancestor.first !== prefix) return corrupt();
        this.saveNode(up, { ...ancestor, first: node.next });
      } else {
        const previous = this.node(node.previous) ?? corrupt();
        if (previous.next !== prefix) return corrupt();
        this.saveNode(node.previous, { ...previous, next: node.next });
      }
      if (node.next !== null) {
        const next = this.node(node.next) ?? corrupt();
        if (next.previous !== prefix) return corrupt();
        this.saveNode(node.next, { ...next, previous: node.previous });
      }
      this.storage.delete(`path:${prefix}`);
      prefix = up;
      node = this.node(prefix) ?? corrupt();
    }
  }

  /** Initial revision is zero. Replacement requires that exact decimal revision;
   * a successful changed set advances it once. Retrying that same replacement
   * is idempotent. A caller must recheck its original input after registration. */
  async register(actor: string, paths: string[], expected?: string): Promise<Placement> {
    if (!actor || paths.some(path => typeof path !== "string" || !path)
      || new Set(paths).size !== paths.length || (expected !== undefined && !revision(expected))) {
      return { refused: "invalid_argument" };
    }
    let changed = false;
    const result = this.storage.transaction<Placement>(() => {
      const index = this.index(), existing = this.registration(actor);
      if (existing) {
        const prior = this.paths(actor, existing.pathCount), desired = new Set(paths);
        const same = prior.length === paths.length && prior.every(path => desired.has(path));
        if (same && (expected === undefined || expected === existing.revision
          || (BigInt(expected) + 1n).toString() === existing.revision)) return { admitted: true };
        if (expected !== existing.revision) return { refused: "conflict" };
        if (!Number.isSafeInteger(existing.generation + 1)) return corrupt();
        const previous = new Set(prior);
        for (const path of prior) if (!desired.has(path)) this.remove(path, actor);
        for (const path of paths) if (!previous.has(path)) this.insert(path, actor);
        // An acknowledgement captured before replacement cannot erase work
        // retained for the new subscription, even without a later append.
        this.storage.put(`paths:${actor}`, JSON.stringify(paths));
        this.save(actor, { ...existing, pathCount: paths.length, revision: (BigInt(existing.revision) + 1n).toString(),
          generation: existing.generation + 1 });
      } else {
        if (expected !== undefined) return { refused: "conflict" };
        if (this.maximumActors !== undefined && index.actors >= this.maximumActors) return { refused: "capacity" };
        for (const path of paths) this.insert(path, actor);
        this.storage.put(`paths:${actor}`, JSON.stringify(paths));
        this.save(actor, { pathCount: paths.length, revision: "0", generation: 0, pending: false, previous: null, next: null });
        this.saveIndex({ ...index, actors: index.actors + 1 });
      }
      changed = true;
      return { admitted: true };
    });
    if (changed) {
      try { this.durable = this.storage.flush(); }
      catch (error) { this.durable = Promise.reject(error); }
    }
    if ("admitted" in result) await this.durable;
    return result;
  }

  private owners(prefix: string, node: Node, actors: Set<string>): void {
    const visited = new Set<string>();
    let previous: string | null = null;
    for (let actor = node.owner; actor !== null;) {
      if (visited.has(actor)) return corrupt();
      visited.add(actor);
      const member = this.member(prefix.slice(0, -1), actor);
      if (member.previous !== previous) return corrupt();
      actors.add(actor);
      previous = actor;
      actor = member.next;
    }
  }

  private select(prefix: string, subtree: boolean, actors: Set<string>, visited: Set<string>): void {
    const selected = this.node(prefix);
    if (!selected) return;
    if (!subtree) { this.owners(prefix, selected, actors); return; }
    const stack = [{ prefix, node: selected }];
    while (stack.length !== 0) {
      const { prefix, node } = stack.pop()!;
      if (visited.has(prefix)) continue;
      visited.add(prefix);
      this.owners(prefix, node, actors);
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
