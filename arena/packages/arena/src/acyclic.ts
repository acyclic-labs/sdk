/** Thin wrapper over the `acyclic` CLI: fork, list, diff, promote, drop. */
import { spawnSync } from "node:child_process";

export interface Fork { id: string; path: string; base: string; label: string }
export interface Change { status: string; path: string }

const LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const FORK = /^fork\s+([0-9a-f]+)\s+\((\w+)\)\s+(\S+)/;
const LIST = /^([0-9a-f]+)\s+(\w+)\s+(.+?)\s+(\S+)\s+base\s+([0-9a-f]+)/;
const CHANGE = /^([AMDR])\s+(.+)$/;

export class Acyclic {
  constructor(readonly repo: string, readonly binary = process.env.ACYCLIC_BIN ?? "acyclic") {}

  run(...args: string[]): string {
    const p = spawnSync(this.binary, ["--repo", this.repo, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 120_000 });
    if (p.error) throw p.error;
    if (p.status !== 0) throw new Error(`acyclic ${args.join(" ")} failed (${p.status}): ${(p.stderr || p.stdout).trim()}`);
    return p.stdout;
  }

  available(): boolean {
    const p = spawnSync(this.binary, ["--version"], { encoding: "utf8" });
    return !p.error && p.status === 0;
  }

  fork(n: number): Fork[] {
    const out = this.run("fork", "-n", String(n));
    const forks: Fork[] = [];
    for (const line of out.split("\n")) {
      const m = FORK.exec(line);
      if (m) forks.push({ id: m[1]!, path: m[3]!, base: "", label: "" });
    }
    if (forks.length !== n) throw new Error(`asked for ${n} forks, parsed ${forks.length}:\n${out}`);
    const bases = new Map(this.forks().map((f) => [f.id, f.base]));
    forks.forEach((f, i) => { f.base = bases.get(f.id) ?? ""; f.label = LABELS[i] ?? String(i); });
    return forks;
  }

  forks(): Fork[] {
    let out: string;
    try { out = this.run("forks"); } catch { out = this.run("fork-list"); }
    const res: Fork[] = [];
    for (const line of out.split("\n")) {
      const m = LIST.exec(line);
      if (m) res.push({ id: m[1]!, path: m[4]!, base: m[5]!, label: "" });
    }
    return res;
  }

  forkDiff(id: string): Change[] {
    return this.run("fork-diff", id).split("\n").map((l) => CHANGE.exec(l)).filter((m): m is RegExpExecArray => !!m).map((m) => ({ status: m[1]!, path: m[2]! }));
  }

  promote(id: string): string { return this.run("promote", id).trim(); }
  drop(id: string): string { return this.run("fork-drop", id).trim(); }
}
