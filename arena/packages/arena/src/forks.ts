/** Where forks come from. acyclic gives O(1) overlay mounts and a three-way merge on promote.
 *  Without acyclic, plain git worktrees do the same job with a copy per fork and a patch on promote,
 *  so the rest of the acyclic sdk is optional. */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Acyclic, type Fork, type Change } from "./acyclic.js";

export interface Forker {
  readonly name: "acyclic" | "git";
  readonly repo: string;
  /** Paths (relative to the repo) that never enter a fork and never count as fork changes: the arena log, .env. */
  exclude?: string[];
  fork(n: number): Fork[];
  diff(fork: Fork): Change[];
  /** Land the fork. `paths` limits a patch-based promote to what the worker changed (not what a probe left behind). */
  promote(fork: Fork, paths?: string[]): string;
  drop(fork: Fork): void;
}

const LABELS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

export class AcyclicForker implements Forker {
  readonly name = "acyclic" as const;
  constructor(private readonly a: Acyclic) {}
  get repo(): string { return this.a.repo; }
  fork(n: number): Fork[] { return this.a.fork(n); }
  diff(f: Fork): Change[] { return this.a.forkDiff(f.id); }
  promote(f: Fork): string { return this.a.promote(f.id); }
  drop(f: Fork): void { try { this.a.drop(f.id); } catch { /* already consumed by promote */ } }
}

function git(cwd: string, args: string[], allowFail = false): string {
  const p = spawnSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 120_000 });
  if (p.error) throw p.error;
  if (p.status !== 0 && !allowFail) throw new Error(`git ${args.join(" ")} failed: ${(p.stderr || p.stdout).trim().slice(0, 300)}`);
  return p.stdout;
}

/** Every path git status reports, from the NUL-delimited machine format: no C-quoting to undo, and a
 *  rename or copy contributes both its new and its original path. */
const statusPaths = (cwd: string): string[] => {
  const fields = git(cwd, ["status", "--porcelain=v1", "-z", "--untracked-files=all"]).split("\0");
  const out: string[] = [];
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    if (f.length < 4) continue;
    const xy = f.slice(0, 2), path = f.slice(3);
    out.push(path);
    if (xy.includes("R") || xy.includes("C")) { const orig = fields[++i]; if (orig) out.push(orig); }
  }
  return out;
};

/** The file's git blob id ("blob <size>\0<bytes>" under SHA-1), so it compares directly with `HEAD:<path>`. */
const fileSha = (path: string): string | null => {
  try {
    const st = statSync(path); if (st.isDirectory()) return "dir";
    const bytes = readFileSync(path);
    return createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
  } catch { return null; }
};

function sameFile(a: string, b: string): boolean {
  try {
    const x = statSync(a), y = statSync(b);
    if (x.isDirectory() || y.isDirectory()) return true;
    return x.size === y.size && readFileSync(a).equals(readFileSync(b));
  } catch { return false; }
}

/** Forks as detached git worktrees of HEAD, with the working tree's uncommitted changes carried over. */
export class GitForker implements Forker {
  readonly name = "git" as const;
  private root: string | null = null;
  private basePaths = new Set<string>();
  /** Content of every dirty or untracked base file at fork time; clean tracked files are at HEAD. */
  private baseShas = new Map<string, string | null>();
  private baseHead = "";
  exclude: string[];
  constructor(readonly repo: string, exclude: string[] = []) { this.exclude = [...new Set([".env", "arena.jsonl", ...exclude])]; }
  private excluded(path: string): boolean { return this.exclude.some((e) => path === e || path.startsWith(e + "/")); }

  fork(n: number): Fork[] {
    const head = git(this.repo, ["rev-parse", "HEAD"]).trim();
    const dirty = git(this.repo, ["diff", "HEAD"]);
    const untracked = git(this.repo, ["ls-files", "--others", "--exclude-standard"]).trim().split("\n").filter(Boolean).filter((f) => !this.excluded(f));
    this.root ??= mkdtempSync(join(tmpdir(), "arena-wt-"));
    this.basePaths = new Set(statusPaths(this.repo));
    this.baseHead = head;
    this.baseShas = new Map([...this.basePaths].map((p) => [p, fileSha(join(this.repo, p))]));
    const forks: Fork[] = [];
    for (let i = 0; i < n; i++) {
      const id = `${head.slice(0, 6)}${i}${Date.now().toString(36).slice(-4)}`;
      const path = join(this.root, id);
      git(this.repo, ["worktree", "add", "--detach", "-q", path, head]);
      if (dirty.trim()) {
        const p = spawnSync("git", ["apply", "--allow-empty", "-"], { cwd: path, input: dirty, encoding: "utf8" });
        if (p.status !== 0) throw new Error(`could not carry uncommitted changes into fork: ${p.stderr}`);
      }
      for (const f of untracked) { spawnSync("mkdir", ["-p", join(path, f, "..")]); spawnSync("cp", ["-R", join(this.repo, f), join(path, f)]); }
      forks.push({ id, path, base: head.slice(0, 12), label: LABELS[i] ?? String(i) });
    }
    return forks;
  }

  /** What the fork changed relative to the base working tree, by file content, so a file already dirty
   *  in the base and edited further in the fork still counts. */
  diff(f: Fork): Change[] {
    // files the base gained after forking (the arena log, editor scratch) are not fork changes
    const paths = new Set([...statusPaths(f.path), ...this.basePaths]);
    const out: Change[] = [];
    for (const path of [...paths].sort()) {
      if (this.excluded(path)) continue;
      const inBase = existsSync(join(this.repo, path)), inFork = existsSync(join(f.path, path));
      if (inBase && inFork) { if (!sameFile(join(this.repo, path), join(f.path, path))) out.push({ status: "M", path }); }
      else if (inFork) out.push({ status: "A", path });
      else if (inBase && this.basePaths.has(path)) out.push({ status: "D", path });
    }
    return out;
  }

  /** What the base held for `rel` when the forks were taken: the recorded content for a dirty or untracked
   *  file, the HEAD blob for a clean tracked file, null for a file that did not exist. */
  private baseShaAtFork(rel: string): string | null {
    if (this.baseShas.has(rel)) return this.baseShas.get(rel)!;
    const blob = git(this.repo, ["rev-parse", "--verify", "-q", `${this.baseHead}:${rel}`], true).trim();
    return blob || null;
  }

  /** Land the fork by syncing its changed files into the base working tree. A patch against the commit
   *  would not apply when the base already carries uncommitted edits, so files are copied, not patched;
   *  the change arrives unstaged, like a hand edit. acyclic's promote does a real three-way merge instead.
   *  A base file edited since the fork was taken is a conflict: nothing is written and the caller hears
   *  which paths clashed, rather than a newer local edit being silently overwritten. */
  promote(f: Fork, paths?: string[]): string {
    const changes = (paths && paths.length ? paths : this.diff(f).map((c) => c.path)).filter((p) => !this.excluded(p));
    const conflicts = changes.filter((rel) => {
      const now = fileSha(join(this.repo, rel));
      if (now === "dir") return false;
      return now !== this.baseShaAtFork(rel);
    });
    if (conflicts.length) throw new Error(`promote conflict: edited in the working tree since the fork was taken: ${conflicts.join(", ")}`);
    let n = 0;
    for (const rel of changes) {
      const src = join(f.path, rel), dst = join(this.repo, rel);
      if (existsSync(src)) { if (statSync(src).isDirectory()) continue; mkdirSync(dirname(dst), { recursive: true }); copyFileSync(src, dst); n++; }
      else if (existsSync(dst)) { unlinkSync(dst); n++; }
    }
    return n ? `synced ${n} file(s) from fork ${f.label}` : "nothing to promote";
  }

  drop(f: Fork): void {
    git(this.repo, ["worktree", "remove", "--force", f.path], true);
    try { rmSync(f.path, { recursive: true, force: true }); } catch { /* gone */ }
    git(this.repo, ["worktree", "prune"], true);
  }
}

/** acyclic when it is installed and the repo is initialised, otherwise git worktrees. */
export function pickForker(repo: string, prefer: "auto" | "acyclic" | "git" = "auto", exclude: string[] = []): Forker {
  if (prefer === "git") return new GitForker(repo, exclude);
  const a = new Acyclic(repo);
  const ok = a.available() && (() => { try { a.forks(); return true; } catch { return false; } })();
  if (ok) return new AcyclicForker(a);
  if (prefer === "acyclic") throw new Error("acyclic is not available for this repo (install it and run `acyclic init`)");
  return new GitForker(repo, exclude);
}
