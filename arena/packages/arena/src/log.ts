/** Append-only, hash-chained JSONL. Same record shape as the Python jev SDK. */
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, readFileSync } from "node:fs";

export interface Record_ {
  seq: number;
  ts: string;
  kind: string;
  data: Record<string, unknown>;
  prev: string;
  hash: string;
}

const GENESIS = "0".repeat(64);

function canon(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v);
  if (Array.isArray(v)) return "[" + v.map(canon).join(",") + "]";
  const o = v as Record<string, unknown>;
  return "{" + Object.keys(o).sort().map((k) => JSON.stringify(k) + ":" + canon(o[k])).join(",") + "}";
}

export const sha256 = (s: string): string => createHash("sha256").update(s, "utf8").digest("hex");

export class Log {
  readonly path: string | null;
  private records: Record_[] = [];
  private tip = GENESIS;

  constructor(path: string | null = null) {
    this.path = path;
    if (path && existsSync(path)) {
      for (const line of readFileSync(path, "utf8").split("\n")) {
        if (!line.trim()) continue;
        const r = JSON.parse(line) as Record_;
        this.records.push(r);
        this.tip = r.hash;
      }
    }
  }

  append(kind: string, data: Record<string, unknown>): Record_ {
    const body = { seq: this.records.length, ts: new Date().toISOString(), kind, data, prev: this.tip };
    const r: Record_ = { ...body, hash: sha256(canon(body)) };
    this.records.push(r);
    this.tip = r.hash;
    if (this.path) appendFileSync(this.path, JSON.stringify(r) + "\n");
    return r;
  }

  note(event: string, data: Record<string, unknown> = {}): Record_ {
    return this.append("note", { event, ...data });
  }

  get length(): number { return this.records.length; }
  all(): Record_[] { return [...this.records]; }
  kind(k: string): Record_[] { return this.records.filter((r) => r.kind === k); }

  verify(): void {
    let prev = GENESIS;
    this.records.forEach((r, i) => {
      if (r.seq !== i) throw new Error(`record ${i}: seq is ${r.seq}`);
      if (r.prev !== prev) throw new Error(`record ${i}: broken chain`);
      const { hash, ...body } = r;
      if (sha256(canon(body)) !== hash) throw new Error(`record ${i}: hash mismatch (edited?)`);
      prev = hash;
    });
  }
}
