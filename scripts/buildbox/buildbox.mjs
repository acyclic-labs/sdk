// buildbox.mjs: run a build-slot remote job on the build box (bb2, AWS c6a.metal) (2026-10-15, bb lane). Imported by
// build-slot-remote.mjs `run`, which tries this first and falls back to Daytona only when it returns 222.
//
//   ssh alias `buildbox` (~/.ssh/config -> ~/.ssh/buildbox/config): user var, key ~/.ssh/buildbox/id_ed25519.
//   Box: AWS us-west-2 bb2 (persistent spot; see /home/var/bin/buildbox-ctl). No Cloud credentials on it.
//
// Per job (git sync, 2026-10-15: the host uplink is ~150 KB/s and full-tree rsyncs starved every lane): one ssh carries
// a few-KB payload (git bundle of the commits the VM lacks, the tree identity's snapshot as a binary diff, untracked files included) and the launch; the
// VM (/usr/local/bin/bb-git-prep) fetches the bundle into its mirror /home/var/mirror/cloud.git (no GitHub credentials on
// the VM: commits arrive only as bundles) and checks out the claimed worktree /mnt/bb/wt/<key>.<n> at that state. What
// the VM has is cached here (VM_HAVE); a stale cache costs one retry (the VM answers with its SHAs). Not a git checkout,
// or the git path fails: incremental rsync to /mnt/bb/src/<key>.<n> as before (excluding .git, target/, node_modules,
// managed-agents/ and anything .gitignore'd). Then the command runs in a private mount namespace where /work/src is that copy and /work/target its
// job-owned target dir (removed at exit), as the unprivileged user var. Every lane compiles at the same
// paths, so one sccache disk cache (persistent OS disk) hits across lanes; each job has its own sccache server.
//
// Detached jobs (2026-10-15 fix, "lost build-box connection"): the job runs detached on the VM with stdout/stderr in
// files; this side follows them with `tail -F` over plain ssh connections (no shared ControlMaster: a hung master hung
// every client) and reattaches at the byte offset it has after any drop, for up to REATTACH_SEC. The host's Wi-Fi
// uplink stalls for minutes under load, so a dropped connection is not a lost job. A VM-side watchdog kills the job if
// this side stops heartbeating for 15 min (the caller died). stdout/stderr stay separate and the rc is the job's.
//
// Copy-back (2026-10-15 fix): nothing comes back unless declared. BUILD_SLOT_REMOTE_FETCH lists output paths (files or
// directories; relative to the cwd, or /work/target/... -> the local CARGO_TARGET_DIR); after a pass only files under
// them that the job itself wrote (newer than the job's start) come back. Source files never come back.
//
// Tree identity (2026-10-16): every job prints BUILD_SLOT_TREE=<commit> <tree> <content digest> and refuses to run (224
// tree-mismatch) unless the VM checkout has exactly the client's content (see treeIdentity). Host-shaped target dirs
// (/home/var/targets/<label>) are bound per source tree inside the job, like /work/target.
//
// Unreachable VM: returns 222 at once (Daytona takes this call) and starts `buildbox-ctl heal` in the background: refresh
// the NSG's source IP, start the VM if the idle watcher deallocated it. BUILD_SLOT_BUILDBOX=0 skips the VM for one call.
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import zlib from 'node:zlib';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { arm, atomic, finish } from './buildbox-receipt.mjs';
const RECEIPT_TOOL = fs.readFileSync(new URL('./buildbox-receipt.mjs', import.meta.url), 'utf8');

// One build box (user, 2026-10-21): the pool is bb2 and only bb2; "we only ever stay at a single build box". Every other
// host is refused with rc 2, so no override can route a job elsewhere.
// `buildbox` is bb2's ssh alias.
const BOX = 'bb2';
const BOXES = [BOX];
let HOST = BOX;
for (const h of [process.env.BUILD_SLOT_BUILDBOX_HOST, ...(process.env.BUILD_SLOT_BUILDBOXES || '').split(/\s+/)].filter(Boolean)) {
  if (h === BOX || h === 'buildbox') continue;
  process.stderr.write(`[build-slot] the only build box is ${BOX}; drop BUILD_SLOT_BUILDBOX_HOST/BUILD_SLOT_BUILDBOXES (=${h})\n`);
  process.exit(2);
}
const primary = (h) => h === 'bb2' || h === 'buildbox';
const boxSfx = (h) => primary(h) ? '' : `.${h}`;
const disabledMark = (h) => `/home/var/.ssh/buildbox/disabled${primary(h) ? '' : `-${h}`}`;
const UNAVAILABLE = 222;
// Why the last run() returned UNAVAILABLE, for strict --remote callers (build-slot-remote.mjs exits 224 naming it):
// host-tools-missing | no-checkout | copies-busy | vm-unreachable | sync-failed | launch-failed | job-lost. Empty when
// run() returned the job's own rc.
export let cause = ''; export let causeDetail = '';
const unavailable = (c, d = '') => { cause = c; causeDetail = String(d).slice(0, 240); return UNAVAILABLE; };
const LOGDIR = '/home/var/log/buildbox';
const SSH = ['-F', '/home/var/.ssh/buildbox/config', '-o', 'BatchMode=yes', '-o', 'ControlPath=none'];
const REATTACH_SEC = Number(process.env.BUILD_SLOT_BUILDBOX_REATTACH_SEC || 1200);
// Windows adapter (2026-10-26): bs.ps1 runs only `snapshot`/`snapshot-attest` here with native git.exe; the shared WSL
// client does everything else. No log, ssh or cache state is created on the Windows side.
const WIN = process.platform === 'win32';
const CACHE = process.env.BUILD_SLOT_CLIENT_CACHE || (WIN ? path.join(os.tmpdir(), 'buildbox') : '/home/var/.cache/buildbox');
if (!WIN) fs.mkdirSync(LOGDIR, { recursive: true });
const ts = () => new Date().toISOString().slice(11, 19);
const say = (m) => process.stderr.write(`[build-slot buildbox] ${ts()} ${m}\n`);
const q = (s) => /^[A-Za-z0-9_@%+=:,./-]+$/.test(s) ? s : `'${String(s).replaceAll("'", `'\\''`)}'`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// Per-phase client timing (2026-10-26): ms since this node process started, one BUILD_SLOT_PHASES line per job and the
// same object in the client receipt, so admission/transfer/prep/teardown are measured, never inferred from wall time.
const PHASES = {};
const mark = (name) => { PHASES[name] = Math.round(performance.now()); };

export const enabled = () => process.env.BUILD_SLOT_BUILDBOX !== '0' && fs.existsSync('/home/var/.ssh/buildbox/config') &&
  !fs.existsSync(disabledMark(BOX));  // buildbox-ctl stop
// The idle watcher (buildbox-ctl idle) stops the VM after 60 min with no running job; this marks use.
const used = () => { const f = `${LOGDIR}/last-use${primary(HOST) ? '' : `-${HOST}`}`; try { fs.closeSync(fs.openSync(f, 'a')); const n = new Date(); fs.utimesSync(f, n, n); } catch {} };

const REACH = '/home/var/.cache/buildbox/reach';
function heal(why, h = HOST) {
  say(`${h} unreachable (${why}); starting buildbox-ctl${primary(h) ? '' : ` ${h}`} heal in the background`);
  try { if (fs.readFileSync(`${REACH}-${h}`, 'utf8').startsWith('ok')) fs.unlinkSync(`${REACH}-${h}`); } catch {}
  try { fs.appendFileSync(`${LOGDIR}/heal.log`, `${new Date().toISOString()} pid=${process.pid} box=${h} cause: ${why}\n`); } catch {}
  try {
    const out = fs.openSync(`${LOGDIR}/heal.log`, 'a');
    spawn('/home/var/bin/buildbox-ctl', primary(h) ? ['heal'] : [h, 'heal'], { detached: true, stdio: ['ignore', out, out] }).unref();
  } catch {}
}
function setHost(h) {
  HOST = h; VM_HAVE = `/home/var/.cache/buildbox/vm-have${boxSfx(h)}`; VM_HAVE_MAIN = `/home/var/.cache/buildbox/vm-have-main${boxSfx(h)}`;
  DISK_STAMP = `/home/var/.cache/buildbox/disk-check${boxSfx(h)}`;
}
// '' when box h answers (memoised), else why not. A fresh failure starts that box's heal.
async function probe(h) {
  const f = `${REACH}-${h}`;
  try { const v = fs.readFileSync(f, 'utf8').trim(); if (Date.now() - fs.statSync(f).mtimeMs < (v === 'ok' ? 60e3 : 90e3)) return v === 'ok' ? '' : v.replace(/^down ?/, '') || 'down'; } catch {}
  const r = await sshRun('mountpoint -q /mnt/bb && echo ok', { timeout: 30000, host: h, connect: 10 });
  const why = r.status === 0 && /ok/.test(r.stdout.toString()) ? '' : (r.stderr.trim().split('\n').pop() || `ssh rc ${r.status}`).slice(0, 160);
  if (why) heal(why, h);
  try { fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(`${f}.${process.pid}`, why ? `down ${why}` : 'ok'); fs.renameSync(`${f}.${process.pid}`, f); } catch {}
  return why;
}
// Picks the first enabled box that answers; '' or why none does.
async function pickHost() {
  const live = BOXES.filter((h) => !fs.existsSync(disabledMark(h))); const notes = [];
  // One enabled box (bb2 only, 2026-10-18): the launch connection itself is the probe (2026-10-26 admission: a separate
  // probe ssh cost 2.1 s of handshake whenever its 60 s cache was stale). A failed launch heals exactly as a failed probe.
  if (live.length === 1) { setHost(live[0]); return ''; }
  for (const h of live) {
    const why = await probe(h);
    if (!why) { setHost(h); if (notes.length) say(`using ${h} (${notes.join('; ')})`); return ''; }
    notes.push(`${h}: ${why}`);
  }
  if (live.length) setHost(live[0]);
  return notes.join('; ') || 'the build box is disabled (buildbox-ctl start re-enables)';
}

// Disk gate (2026-10-18 infra-bbdisk; 10:45Z /mnt/bb hit 100% and every lane's remote build failed with ENOSPC): before
// admission, at most once per DISK_EVERY_MS across all jobs (stamp file; every job while the last reading was >= 95%),
// ask the box for /mnt/bb usage. Above 85% bb-disk-sweep --check starts its age-based sweep of idle target files
// (6 h, 4, 3, 2, 1 h until <= 75%) in the background; at >= 95% this job waits for it (max 100 s). Never fails a job.
let DISK_STAMP = '/home/var/.cache/buildbox/disk-check'; const DISK_EVERY_MS = 60e3;
async function diskGate(argv) {
  const cls = jobClass(argv); if (!cls || cls === 'light') return;
  try { const s = fs.statSync(DISK_STAMP); if (Date.now() - s.mtimeMs < DISK_EVERY_MS && Number(fs.readFileSync(DISK_STAMP, 'utf8')) < 95) return; } catch {}
  try { fs.mkdirSync(path.dirname(DISK_STAMP), { recursive: true }); fs.writeFileSync(DISK_STAMP, '0'); } catch { return; }
  const r = await sshRun('/usr/local/sbin/bb-disk-sweep --check --wait 100 --reason admit 2>&1', { timeout: 115000 });
  const o = r.stdout.toString().trim(); const pct = Number(/(\d+)% used/.exec(o)?.[1] ?? 0);
  try { fs.writeFileSync(DISK_STAMP, String(pct)); } catch {}
  if (pct > 85 || r.status !== 0) say(`disk gate: ${o.replace(/\s+/g, ' ').slice(0, 240) || `ssh rc ${r.status}`}`);
}

// Short remote command over its own connection; resolves {status, stdout, stderr}. Never blocks the event loop.
function sshRun(cmd, { input, timeout = 60000, host = HOST, connect = 15 } = {}) {
  return new Promise((resolve) => {
    const p = spawn('ssh', [...SSH, '-o', `ConnectTimeout=${connect}`, '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3', host, cmd], { stdio: ['pipe', 'pipe', 'pipe'] });
    const out = [], err = []; let done = false;
    const t = setTimeout(() => { if (!done) p.kill('SIGKILL'); }, timeout);
    p.stdout.on('data', (b) => out.push(b)); p.stderr.on('data', (b) => err.push(b));
    p.on('error', (e) => { err.push(Buffer.from(e.message)); });
    p.stdin.on('error', (e) => { err.push(Buffer.from(`stdin: ${e.message}\n`)); });  // remote exited early: its rc says why
    p.on('close', (code) => { done = true; clearTimeout(t); resolve({ status: code ?? 255, stdout: Buffer.concat(out), stderr: Buffer.concat(err).toString() }); });
    p.stdin.end(input ?? '');
  });
}

// Pre-connected launch transport (2026-10-26, admission < 5 s): `bash -s` on the one enabled box, connected while the
// client attests and identifies the tree; the launch script is written to its stdin once known. cancel() ends stdin
// empty, so a job that replays a verdict or is refused runs nothing remotely. A failed connection resolves like a
// failed sshRun, so gitPrepLaunch's retry and unreachable handling are unchanged.
function preconnect(host = HOST) {
  const p = spawn('ssh', [...SSH, '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3', host, 'bash -s'], { stdio: ['pipe', 'pipe', 'pipe'] });
  const out = [], err = []; let sent = false, timer = null;
  p.stdout.on('data', (b) => out.push(b)); p.stderr.on('data', (b) => err.push(b));
  p.on('error', (e) => { err.push(Buffer.from(e.message)); });
  p.stdin.on('error', (e) => { err.push(Buffer.from(`stdin: ${e.message}\n`)); });
  const closed = new Promise((resolve) => p.on('close', (code) => { clearTimeout(timer); resolve({ status: code ?? 255, stdout: Buffer.concat(out), stderr: Buffer.concat(err).toString() }); }));
  return {
    host,
    send(input, timeout = 60000) { if (sent) throw new Error('pre-connected transport already used'); sent = true; timer = setTimeout(() => p.kill('SIGKILL'), timeout); p.stdin.end(input); return closed; },
    cancel() { if (!sent) { sent = true; p.stdin.end(''); } },
  };
}

async function reachable() {
  let last = '';
  for (let i = 0; i < 2; i++) {  // one retry: a slow handshake under host load is not an outage
    const r = await sshRun('mountpoint -q /mnt/bb && echo ok', { timeout: 45000 });
    if (r.status === 0 && /ok/.test(r.stdout.toString())) return '';
    last = (r.stderr.trim().split('\n').pop() || `ssh rc ${r.status}${r.status === 255 ? '' : ' (scratch not mounted)'}`).slice(0, 160);
  }
  return last;
}

// Repos (2026-10-15, multi-repo): the repo is the checkout's origin (github.com/acyclic-labs/<name>); anything else is
// cloud, as before. Per repo: VM mirror /home/var/mirror/<name>.git, VM-have caches, worktrees, target keys. cloud keeps
// its original paths. sdk is public: the VM fetches GitHub itself (bb-git-prep, buildbox-mirror-sync), so a job never
// needs rsync; sdk never rsyncs (a failed git sync is a named failure), and its jobs get bun on PATH.
const REPOS = {
  cloud: { wt: '/mnt/bb/wt', tkey: '', have: '', rsync: true, path: '' },
  sdk: { wt: '/mnt/bb/wt-sdk', tkey: 'sdk/', have: '.sdk', rsync: false, path: '/home/var/.bun/bin:' },
};
// Checkouts on Windows drives (/mnt/<drive>/..., e.g. lanes in Q:\wt\<lane>) use WSL git only, never git.exe
// (2026-10-18 gitfix): git.exe through WSL interop hung for hours with the Windows process already gone (the /init relay
// never saw it exit; spawnSync had no timeout), holding the build-slot claim. Two drvfs fixes make WSL git work there:
// - A worktree made from Windows has `.git: gitdir: Q:/cloud/.git/worktrees/<name>`, which WSL git cannot read. gitEnvFor
//   translates it to /mnt/q/... and runs git with GIT_DIR/GIT_WORK_TREE (GIT_COMMON_DIR when commondir is a Windows path
//   too) in each child's env only: never exported, never written to any config.
// - The index Windows git wrote has no inode/uid and Windows ctimes, so WSL git rehashed every file (`git add -A` 35 s on
//   a cloud worktree; 79 s `git diff HEAD` in Q:\sdk). core.checkStat=minimal + core.trustCtime=false (env-only config)
//   compare whole-second mtime and size, as git.exe does: 3 s, identical index (2026-10-18, cloud and sdk worktrees).
// Every git call has a hard timeout and a closed stdin, and fails with the command named.
const GIT_TIMEOUT_MS = 120000;
const DRVFS_GIT_CFG = [['core.checkStat', 'minimal'], ['core.trustCtime', 'false']];
const WIN_EOL_KEYS = '^core\\.(autocrlf|eol|safecrlf)$';
const WIN_GIT_SYSTEM = '/mnt/c/Program Files/Git/etc/gitconfig';
const WIN_GIT_GLOBAL = process.env.BUILD_SLOT_WIN_GITCONFIG || '/mnt/c/Users/varun/.gitconfig';
const winPath = (p) => { const m = /^([A-Za-z]):[\\/](.*)$/.exec(p.trim()); return m ? `/mnt/${m[1].toLowerCase()}/${m[2].replace(/\\/g, '/')}` : null; };
// The env git needs for cwd's checkout: {} for a checkout WSL git reads as is; {GIT_DIR, GIT_WORK_TREE[, GIT_COMMON_DIR]}
// when its .git file names a Windows gitdir; plus the drvfs stat config under /mnt/<drive>.
function gitEnvFor(cwd) {
  const env = {};
  // Native Windows git on NTFS: the fsmonitor daemon and untracked cache answer status/add without walking the tree.
  if (WIN) {
    const cfg = [['core.fsmonitor', 'true'], ['core.untrackedCache', 'true']], n = Number(process.env.GIT_CONFIG_COUNT) || 0;
    cfg.forEach(([k, v], i) => { env[`GIT_CONFIG_KEY_${n + i}`] = k; env[`GIT_CONFIG_VALUE_${n + i}`] = v; });
    env.GIT_CONFIG_COUNT = String(n + cfg.length); return env;
  }
  for (let d = path.resolve(cwd); ; d = path.dirname(d)) {
    let st; try { st = fs.statSync(`${d}/.git`); } catch { if (d === '/') break; continue; }
    if (st.isFile()) {
      const g = /^gitdir:\s*(.+?)\s*$/m.exec(fs.readFileSync(`${d}/.git`, 'utf8'))?.[1] || '';
      const gd = winPath(g);
      if (gd) {
        env.GIT_DIR = gd; env.GIT_WORK_TREE = d;
        let cd = ''; try { cd = fs.readFileSync(`${gd}/commondir`, 'utf8').trim(); } catch {}
        if (winPath(cd)) env.GIT_COMMON_DIR = winPath(cd);
      }
    }
    break;
  }
  if (/^\/mnt\/[a-z]\//.test(env.GIT_WORK_TREE || path.resolve(cwd))) {
    // Line-ending config as git.exe sees it: the repo's own value, else Windows global, else Git for Windows' system file
    // (sdk sets none, so git.exe hashed with the Windows global core.autocrlf=input; WSL git would not have).
    const cfg = [...DRVFS_GIT_CFG];
    const get = (file) => { const o = {}; try {
      const r = spawnSync('git', ['config', ...(file ? ['--file', file] : ['--local']), '--get-regexp', WIN_EOL_KEYS], { cwd, env: { ...process.env, ...env }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 30000 });
      if (r.status !== 0 && r.status !== 1) return null;  // 1: no such key; else not a repo (or no such file)
      for (const l of String(r.stdout || '').split('\n')) { const m = /^(\S+) (.*)$/.exec(l); if (m) o[m[1].toLowerCase()] = m[2]; }
    } catch { return null; } return o; };
    const local = get('');
    if (local) for (const [k, v] of Object.entries({ ...get(WIN_GIT_SYSTEM), ...get(WIN_GIT_GLOBAL) })) if (!(k in local)) cfg.push([k, v]);
    const n = Number(process.env.GIT_CONFIG_COUNT) || 0;
    cfg.forEach(([k, v], i) => { env[`GIT_CONFIG_KEY_${n + i}`] = k; env[`GIT_CONFIG_VALUE_${n + i}`] = v; });
    env.GIT_CONFIG_COUNT = String(n + cfg.length);
  }
  return env;
}
function gitIn(top, genv = {}) {
  const env = { ...process.env, GIT_OPTIONAL_LOCKS: '0', GIT_TERMINAL_PROMPT: '0', ...genv };
  return (args, o = {}) => spawnSync('git', args, { cwd: top, env, maxBuffer: 1 << 30, timeout: GIT_TIMEOUT_MS, killSignal: 'SIGKILL', stdio: [o.input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'], ...o });
}
// {top, genv, drvfs, repo, git}: the checkout root, the env its git calls need, whether it is on a Windows drive, its
// repo, a git runner for it.
const checkouts = new Map();
function checkout(cwd) {
  if (!checkouts.has(cwd)) checkouts.set(cwd, lookupCheckout(cwd));
  return checkouts.get(cwd);
}
function lookupCheckout(cwd) {
  const genv = gitEnvFor(cwd);
  let top = genv.GIT_WORK_TREE || '';
  if (!top) try { top = execFileSync('git', ['rev-parse', '--show-toplevel'], { cwd, env: { ...process.env, ...genv }, stdio: ['ignore', 'pipe', 'ignore'], timeout: GIT_TIMEOUT_MS }).toString().trim(); } catch {}
  if (!top) top = path.dirname(execFileSync('cargo', ['locate-project', '--workspace', '--message-format', 'plain'], { cwd, stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim());
  const git = gitIn(top, genv);
  const url = String(git(['config', '--get', 'remote.origin.url'], { encoding: 'utf8' }).stdout || '').trim();
  return { top, genv, drvfs: /^\/mnt\/[a-z]\//.test(top), repo: repoOf(url), git };
}
function repoOf(url) {
  const name = /github\.com[:/]acyclic-labs\/([A-Za-z0-9_.-]+?)(\.git)?\/?$/.exec(url)?.[1];
  return REPOS[name] ? name : 'cloud';
}
// The repo of cwd ('cloud' when unknown); build-slot-remote.mjs sends every non-cloud job down the strict VM path.
export const repoAt = (cwd) => { try { return checkout(cwd).repo; } catch { return 'cloud'; } };
const keyOf = (p) => `${path.basename(p).replace(/[^A-Za-z0-9_.-]/g, '_').slice(0, 60)}-${crypto.createHash('sha256').update(p).digest('hex').slice(0, 8)}`;

// rsync: changed files only; --delete keeps excluded dirs (node_modules, target, .git) on the VM. A new copy is seeded
// VM-side from another synced tree (/usr/local/bin/bb-rsync-recv), so only the delta crosses the uplink.
function sync(co, key, files) {
  const t0 = Date.now(); const top = co.top;
  // The sender sends exactly the identity's file list (2026-10-16 treeid): rsync's own .gitignore filter is not git's (no
  // `!` negation, ignores tracked files a .gitignore matches), so every rsynced cloud copy lacked web/.dev.vars.example and
  // web/src/routes/api/leads/+server.ts. The .gitignore files stay a receiver-side filter only, protecting ignored outputs
  // in the VM copy (web/build, node_modules) from --delete.
  const esc = (s) => /[*?[]/.test(s) ? s.replace(/[*?[\\]/g, '\\$&') : s;
  const rules = new Set();
  for (const f of files) {
    const parts = f.split('/');
    for (let i = 1; i < parts.length; i++) rules.add(`S /${esc(parts.slice(0, i).join('/'))}/`);
    rules.add(`S /${esc(f)}`);
  }
  const rf = `/home/var/.cache/buildbox/sync-rules-${process.pid}-${Date.now()}`;
  fs.writeFileSync(rf, `${[...rules].join('\n')}\n`);
  try { return syncWith(top, key, rf, t0); } finally { fs.rmSync(rf, { force: true }); }
}
function syncWith(top, key, rf, t0) {
  // At most 6 uploads at once host-wide (first free slot, bb-slot-run; was pid%3, which queued on busy slots): the host uplink is ~150 KB/s and parallel uploads starved
  // every ssh handshake (25-95 s logins, 2026-10-15).
  fs.mkdirSync(`${LOGDIR}/sync-slots`, { recursive: true });
  // -rlpc, no -t: changed files land with a fresh mtime and unchanged ones (by checksum) are untouched, so a changed file's
  // mtime never goes backwards for cargo (client mtimes can be older than the last build: git checkout, tar restores).
  const r = spawnSync('/home/var/bin/bb-slot-run', [`${LOGDIR}/sync-slots`, String(Number(process.env.BUILD_SLOT_BUILDBOX_SYNC_SLOTS || 6)), 'rsync', '-rlpcz', '--delete', '--info=stats2', '--timeout=300', '-e', `ssh ${SSH.join(' ')} -o ConnectTimeout=15`,
    '--rsync-path', `/usr/local/bin/bb-rsync-recv /mnt/bb/src/${key}`, '--exclude=/.bb-synced',
    '--exclude=/.git', '--exclude=target/', '--exclude=node_modules', '--exclude=/managed-agents/', '--exclude=.wsl-sync-*',
    `--filter=merge ${rf}`, '--filter=:-r .gitignore', '--filter=H *', `${top}/`, `${HOST}:/mnt/bb/src/${key}/`], { encoding: 'utf8', timeout: 900000, maxBuffer: 1 << 26 });
  if (r.status !== 0) throw new Error(`rsync rc ${r.status}${r.error ? ` (${r.error.code})` : ''}: ${(r.stderr || '').trim().slice(-200)}`);
  const sent = /Total bytes sent: ([\d,.]+)/.exec(r.stdout)?.[1] || '?';
  const files = /Number of regular files transferred: ([\d,.]+)/.exec(r.stdout)?.[1] || '?';
  return `rsync ${files} files, ${sent} B sent, ${((Date.now() - t0) / 1000).toFixed(1)}s`;
}

// BUILD_SLOT_REMOTE_ENV="K1 K2" (bs.ps1 --env K=V) forwards those names too, whatever their prefix.
const extraEnv = () => new Set((process.env.BUILD_SLOT_REMOTE_ENV || '').split(/[\s,]+/).filter((k) => /^[A-Za-z_]\w*$/.test(k)));
const passEnv = (extra = extraEnv()) => Object.entries(process.env).filter(([k]) => !/^(HOME|XDG_.*|TMPDIR|SCCACHE_.*|BB_JOB_.*|GATE_(MAIN|FEATURE|WINDOWS_RUNTIME)_TARGET_DIR|XWIN_CACHE_DIR|BB_ADMIT.*|BUILD_SLOT_RECEIPT_.*|BUILD_SLOT_DEFER_EXEC|BUILD_SLOT_EXEC_IN_PAYLOAD)$/.test(k) && (extra.has(k) || /^(BUILD_SLOT_INPUTS_JSON$|BUILD_SLOT_INPUT_BASE$|RUST|CARGO_|NEXTEST_|CLOUD_|PROPTEST_|BB_CRIT_REASON$)/.test(k) &&
  !/^(CARGO_TARGET_DIR|CARGO_BUILD_TARGET_DIR|CARGO_HOME|RUSTUP_HOME|RUSTC|RUSTDOC|RUSTC_WRAPPER|RUSTC_WORKSPACE_WRAPPER|CARGO_BUILD_RUSTC.*|CARGO_BUILD_RUSTDOC|CARGO_BUILD_JOBS|CARGO_INCREMENTAL|RUSTUP_TOOLCHAIN|CARGO_TARGET_.*|CARGO_HOST_.*|CARGO_BUILD_TARGET|CARGO_INSTALL_ROOT|CARGO_MAKEFLAGS)$/.test(k)))
  .map(([k, v]) => `export ${k}=${q(v)}`).join('\n');

// Launch (one short ssh): job files under J=/mnt/bb/jobs/<id>: .sh (the job; present while it runs, counted by the
// idle watcher), .t0 (start marker for copy-back), .hb (client heartbeat), .wpid (wrapper), .pid (job group), .out/.err,
// .rc (written last). The wrapper waits for memory (MemAvailable >= 24G), runs the job in its own process group, kills
// it if .hb goes 15 min stale, then writes .rc.
// --- git sync -------------------------------------------------------------------------------------------------------
let VM_HAVE = '/home/var/.cache/buildbox/vm-have';  // commit SHAs the VM mirror has (newest first)
const MAX_PAYLOAD = 4 << 20;  // larger (VM far behind main, or a foreign repo): rsync's delta is cheaper on this uplink
// + VM_HAVE_MAIN: GitHub main's first-parent SHAs the VM has (buildbox-mirror-sync keeps the VM at main, every 3 min).
let VM_HAVE_MAIN = '/home/var/.cache/buildbox/vm-have-main';
// Per repo: cloud reads vm-have / vm-have-main; sdk reads vm-have.sdk / vm-have-main.sdk (REPOS[repo].have).
function readHave(co, head = '') {
  const sfx = REPOS[co.repo].have; let l = [];
  for (const f of [VM_HAVE + sfx, VM_HAVE_MAIN + sfx]) try { l.push(...fs.readFileSync(f, 'utf8').split('\n').filter((s) => /^[0-9a-f]{40}$/.test(s))); } catch {}
  // HEAD already on the VM (2026-10-20 lane-loop): no bundle is needed, so the filter below is skipped (it cost 14-21 s of
  // rev-parse + rev-list over drvfs per job). A stale entry costs one retry (bb-git-prep 97), as any stale VM-have does.
  if (head && l.includes(head)) return [head];
  // The usual case, a few commits on top of what the VM has: the nearest VM-held ancestor is the only bundle base needed.
  // 400 negatives made `git bundle create` walk for ~16 s over drvfs; one takes ~2 s (2026-10-20 lane-loop).
  if (head && l.length) {
    const r = co.git(['rev-list', '--max-count=200', head], { encoding: 'utf8' });
    if (r.status === 0) { const s = new Set(l); const b = String(r.stdout || '').split('\n').find((x) => s.has(x)); if (b) return [b]; }
  }
  // A public repo's origin/main is on GitHub, which the VM fetches when a bundle base is missing (bb-git-prep): always a
  // valid base, also before buildbox-mirror-sync has run once.
  if (!REPOS[co.repo].rsync) for (const ref of ['refs/remotes/origin/main', 'refs/remotes/origin/HEAD']) {
    const s = String(co.git(['rev-parse', '-q', '--verify', `${ref}^{commit}`], { encoding: 'utf8' }).stdout || '').trim();
    if (/^[0-9a-f]{40}$/.test(s)) { l.push(s); break; }
  }
  if (!l.length) return [];
  // Not a partial clone (2026-10-20 lane-loop): nothing is lazily fetched, so one cat-file lookup in the pack indexes
  // keeps the SHAs held locally (any local commit is a valid bundle prerequisite) instead of walking 50k commits (~15 s
  // over drvfs).
  if (co.git(['config', '--get', 'extensions.partialclone']).status === 1) {
    const u = [...new Set(l)];
    const r = co.git(['cat-file', '--batch-check=%(objectname) %(objecttype)'], { input: `${u.join('\n')}\n`, encoding: 'utf8' });
    if (r.status === 0) { const ok = new Set(String(r.stdout || '').split('\n').filter((s) => / commit$/.test(s)).map((s) => s.slice(0, 40))); return u.filter((s) => ok.has(s)); }
  }
  // Local reachability, not cat-file: in a partial clone cat-file lazily fetches each missing SHA from the origin, one
  // round trip each (~36 s per job, 2026-10-15; git 2.34 has no --no-lazy-fetch). Commits are always local in blob:none clones.
  // On a Windows drive, HEAD and origin/main only: Q:\cloud has ~13k loose refs, and listing them over drvfs took 35 s
  // (only ancestors of HEAD can trim the bundle anyway; origin/main keeps main's SHAs usable as before).
  const refs = co.drvfs ? ['HEAD', ...(co.git(['rev-parse', '-q', '--verify', 'refs/remotes/origin/main^{commit}']).status === 0 ? ['refs/remotes/origin/main'] : [])] : ['HEAD', '--branches', '--remotes'];
  const r = co.git(['rev-list', '--max-count=50000', ...refs], { encoding: 'utf8', maxBuffer: 1 << 28 });
  if (r.status !== 0) throw new Error(`git rev-list for the VM-have filter rc ${r.status}${r.error ? ` (${r.error.code})` : ''}: ${String(r.stderr || '').trim().slice(-160)}`);
  const local = new Set((r.stdout || '').split('\n').map((s) => s.trim()));
  return [...new Set(l)].filter((s) => local.has(s));
}
function writeHave(co, shas, replace) {
  const f = VM_HAVE + REPOS[co.repo].have;
  let l = []; if (!replace) try { l = fs.readFileSync(f, 'utf8').split('\n').filter(Boolean); } catch {}
  const t = `${f}.${process.pid}`;
  try { fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(t, `${[...new Set([...shas, ...l])].slice(0, 400).join('\n')}\n`); fs.renameSync(t, f); } catch {}
}
// The payload for bb-git-prep: {head, buf (gzipped tar), desc}. Throws when the checkout cannot use the git path.
// The tree is the identity snapshot (id.commit + id.patch, which carries untracked files as new files), not the live
// checkout. A bundle is cut from the live HEAD, so it is used only while HEAD is still id.commit.
function gitBundle(co, have, head) {
  const { git: g } = co;
  const live = () => String(g(['rev-parse', '--verify', '-q', 'HEAD']).stdout || '').trim();
  if (live() !== head) throw new Error('HEAD moved since the tree identity');
  const b = g(['bundle', 'create', '-q', '-', '--stdin'], { input: `HEAD\n${have.map((h) => `^${h}\n`).join('')}` });
  if (b.status !== 0 && !/empty bundle/.test(String(b.stderr))) throw new Error(`git bundle rc ${b.status}${b.error ? ` (${b.error.code}, ${GIT_TIMEOUT_MS / 1000}s cap)` : ''}: ${String(b.stderr || '').trim().slice(-160)}`);
  if (live() !== head) throw new Error('HEAD moved since the tree identity');
  return b.status === 0 && b.stdout.length ? b.stdout : null;
}
// bundled: the bundle an attested client snapshot already cut for exactly this have list (null: none needed).
function gitPayload(co, have, id, bundled) {
  const head = id.commit;
  if (!/^[0-9a-f]{40}$/.test(head)) throw new Error('no HEAD commit');
  fs.mkdirSync(CACHE, { recursive: true });
  const d = fs.mkdtempSync(`${CACHE}/pay-`); const parts = [];
  try {
    if (!have.includes(head)) {
      const b = bundled !== undefined ? bundled : gitBundle(co, have, head);
      if (b) { fs.writeFileSync(`${d}/b.bundle`, b); parts.push(`bundle ${b.length} B`); }
    }
    if (id.patch?.length) {
      fs.writeFileSync(`${d}/d.patch`, id.patch);
      const added = (id.patch.toString('latin1').match(/^new file mode /gm) || []).length;
      parts.push(`diff ${id.patch.length} B${added ? ` (${added} new files)` : ''}`);
    }
    const tar = spawnSync('tar', ['-czf', '-', '-C', d, '.'], { maxBuffer: 1 << 30 });
    if (tar.status !== 0) throw new Error(`payload tar rc ${tar.status}`);
    // No rsync alternative (sdk): send larger payloads too, up to 32 MB.
    const cap = REPOS[co.repo].rsync ? MAX_PAYLOAD : 32 << 20;
    if (tar.stdout.length > cap) throw new Error(`payload ${tar.stdout.length} B is over ${cap}`);
    return { head, buf: tar.stdout, desc: `git ${head.slice(0, 12)}${parts.length ? ` + ${parts.join(', ')}` : ''}, ${tar.stdout.length} B sent` };
  } finally { fs.rmSync(d, { recursive: true, force: true }); }
}
// Tree identity (2026-10-16 treeid; a gate under a shared label passed on another tree's test binary): {commit, tree,
// content}. content is sha256 over `git ls-files -s -z` minus modes (blob + path of every file) of a throwaway index that
// is the checkout's real index plus `git add -A` (so uncommitted and untracked, non-ignored files count), minus what
// neither sync carries (managed-agents, target/, node_modules, .wsl-sync-*, the VM's .bb-synced) and gate output written
// into the checkout while its shards run (.node-gate/, node-gate-env's step logs). The job prelude
// computes the same digest in the VM checkout and refuses to run (224 tree-mismatch) unless it is equal, and on the git
// path also unless the checkout's HEAD is the client's commit. Modes are left out: rsync from a Windows drive marks every
// file executable. New objects go to a temp object dir (alternates: the real one), so the client repo is never written.
const IDENT_ADD = [':(exclude)managed-agents', ':(exclude).bb-synced', ':(exclude,glob)**/.wsl-sync-*', ':(exclude,glob)**/.wsl-sync-*/**', ':(exclude,glob)**/.node-gate/**', ':(exclude,glob)**/node_modules/**', ':(exclude,glob)**/target/**'];
// The same paths, as an ERE over `git ls-files -s` records ("<mode> <blob> <stage>\t<path>"); the VM prelude runs it with
// grep -zvE, so it stays within syntax JS and POSIX ERE read alike.
const IDENT_SKIP = '\t(managed-agents(/|$)|[.]bb-synced$)|(\t|/)[.]wsl-sync-[^/]*(/|$)|(\t|/)[.]node-gate/|(\t|/)node_modules(/|$)|(\t|/)target/';
const IDENT_SKIP_RE = new RegExp(IDENT_SKIP);
// {top-level entry: digest} over mode-less records ("<blob> <stage>\t<path>"). IDENT_DIFF is the same function as a
// script the VM runs on a mismatch: argv[1] its records file (NUL-separated), argv[2] the client's groups (base64 JSON).
function identGroups(recs, createHash) {
  const g = {};
  for (const r of recs) { const k = r.slice(r.indexOf('\t') + 1).split('/')[0]; (g[k] ||= createHash('sha256')).update(`${r}\0`); }
  return Object.fromEntries(Object.entries(g).map(([k, h]) => [k, h.digest('hex').slice(0, 16)]));
}
const IDENT_DIFF = `const fs = require('fs'); ${identGroups.toString()}
const v = identGroups(fs.readFileSync(process.argv[1], 'utf8').split('\\0').filter(Boolean), require('crypto').createHash); const c = JSON.parse(Buffer.from(process.argv[2], 'base64').toString());
const d = [...new Set([...Object.keys(c), ...Object.keys(v)])].filter((k) => c[k] !== v[k]).sort().map((k) => k + (c[k] ? (v[k] ? '' : ' (client only)') : ' (VM only)'));
console.error('[build-slot] tree-mismatch differs under: ' + (d.slice(0, 12).join(', ') || 'nothing (racy)') + (d.length > 12 ? ', +' + (d.length - 12) + ' more' : ''));`;
function treeIdentity(co) {
  const { top, genv = {} } = co;
  fs.mkdirSync(CACHE, { recursive: true });
  const tmp = fs.mkdtempSync(`${CACHE}/ident-`);
  const g = (args, env = {}, ok = false, raw = false) => {
    const e = { ...process.env, GIT_OPTIONAL_LOCKS: '0', GIT_TERMINAL_PROMPT: '0', ...genv, ...env };
    const r = spawnSync('git', args, { cwd: top, env: e, encoding: raw ? 'buffer' : 'utf8', maxBuffer: 1 << 30, timeout: GIT_TIMEOUT_MS, killSignal: 'SIGKILL', stdio: ['ignore', 'pipe', 'pipe'] });
    if (r.status !== 0 && !ok) throw new Error(`git ${args.find((a) => !a.includes('=') && a !== '-c')} rc ${r.status}${r.error ? ` (${r.error.code}, ${GIT_TIMEOUT_MS / 1000}s cap)` : ''}: ${String(r.stderr || r.error?.message || '').trim().slice(-160)}`);
    return r.status !== 0 ? null : raw ? r.stdout : String(r.stdout);
  };
  try {
    let commit = 'none', tree = 'none', env;
    if (g(['rev-parse', '--git-dir'], {}, true) !== null) {
      const h = (g(['rev-parse', '-q', '--verify', 'HEAD'], {}, true) || '').trim();
      if (/^[0-9a-f]{40}$/.test(h)) { commit = h; tree = g(['rev-parse', 'HEAD^{tree}']).trim(); }
      const idx = g(['rev-parse', '--path-format=absolute', '--git-path', 'index']).trim();
      const objs = g(['rev-parse', '--path-format=absolute', '--git-path', 'objects']).trim();
      try { fs.copyFileSync(idx, `${tmp}/index`); } catch {}
      fs.mkdirSync(`${tmp}/objects`);
      env = { GIT_INDEX_FILE: `${tmp}/index`, GIT_OBJECT_DIRECTORY: `${tmp}/objects`, GIT_ALTERNATE_OBJECT_DIRECTORIES: objs };
    } else {  // not a git checkout (snapshot copies): a scratch repo over the tree, honouring its .gitignore like the rsync filter
      execFileSync('git', ['init', '-q', `${tmp}/r`], { stdio: ['ignore', 'ignore', 'pipe'], timeout: GIT_TIMEOUT_MS });
      env = { GIT_DIR: `${tmp}/r/.git`, GIT_WORK_TREE: top, GIT_COMMON_DIR: undefined };
    }
    // The client's line-ending conversion is the VM's for the digest: an rsynced untracked CRLF file arrives raw, and with
    // core.autocrlf=input|true the client hashes it as LF (sdk checkouts on Windows drives, 2026-10-16).
    const autocrlf = (g(['config', '--get', 'core.autocrlf'], env, true) || '').trim().toLowerCase();
    g(['add', '-A', '--', '.', ...IDENT_ADD], env);
    // Declared proof files (often ignored *.log files) belong to the upload's
    // existing content identity. Never validate a local input and omit it from
    // the remote snapshot. The remote's existing forced-path path handles it.
    const inputFiles = sourceInputPaths(co);
    if (inputFiles.length) g(['add', '-f', '--', ...inputFiles], env);
    // Dropped by path (IDENT_SKIP), not `git rm --cached`: git 2.34 leaves skip-worktree entries (sparse checkouts) in place.
    const recs = g(['ls-files', '-s', '-z'], env).split('\0').filter(Boolean).filter((r) => !IDENT_SKIP_RE.test(r));
    const content = crypto.createHash('sha256').update(recs.map((r) => `${r.slice(r.indexOf(' ') + 1)}\0`).join('')).digest('hex');
    // Per top-level entry digests: on a mismatch the prelude names the entries that differ.
    const groups = Buffer.from(JSON.stringify(identGroups(recs.map((r) => r.slice(r.indexOf(' ') + 1)), crypto.createHash))).toString('base64');
    // forced: tracked files a .gitignore matches (web/src/routes/api/leads/+server.ts): an rsynced VM copy's own repo would
    // ignore them, so the prelude adds them with -f. It depends only on the path list, the .gitignore blobs and the exclude
    // files, so it is cached under those (2026-10-18 gitfix: `ls-files -ci` reads every directory's .gitignore, 9 s over
    // drvfs; the key changes only when a file is added, removed or renamed, or an ignore rule changes).
    const readOr = (p) => { try { return p ? fs.readFileSync(p, 'utf8') : ''; } catch { return ''; } };
    const xdg = process.env.XDG_CONFIG_HOME || `${process.env.HOME || ''}/.config`;
    const fkey = crypto.createHash('sha256').update(JSON.stringify(['forced-v1',
      recs.map((r) => r.slice(r.indexOf('\t') + 1)).join('\0'),
      recs.filter((r) => /(^|\/)\.gitignore$/.test(r.slice(r.indexOf('\t') + 1))).map((r) => r.slice(r.indexOf(' ') + 1)),
      readOr((g(['rev-parse', '--path-format=absolute', '--git-path', 'info/exclude'], env, true) || '').trim()),
      readOr((g(['config', '--path', '--get', 'core.excludesFile'], env, true) || '').trim() || `${xdg}/git/ignore`)])).digest('hex');
    const ff = `${CACHE}/forced/${fkey}.json`;
    let forced; try { forced = JSON.parse(fs.readFileSync(ff, 'utf8')); } catch {}
    if (!Array.isArray(forced)) {
      const out = g(['ls-files', '-ci', '--exclude-standard', '-z'], env, true);
      forced = (out || '').split('\0').filter((f) => f && !IDENT_SKIP_RE.test(`\t${f}`));
      if (out !== null) try { fs.mkdirSync(path.dirname(ff), { recursive: true }); fs.writeFileSync(`${ff}.${process.pid}`, JSON.stringify(forced)); fs.renameSync(`${ff}.${process.pid}`, ff); } catch {}
    }
    // The git path ships this snapshot, not the live checkout: the throwaway index against the commit (tracked changes and
    // untracked files alike, as the blobs hashed above), so a checkout edited while its job launches cannot send a tree
    // other than the one identified (2026-10-16: a lane committing and editing mid-gate failed the check, correctly).
    const patch = commit === 'none' ? null : g(['-c', 'diff.noprefix=false', '-c', 'diff.mnemonicPrefix=false', 'diff', '--cached', '--binary',
      '--full-index', '--no-color', '--no-ext-diff', '--no-textconv', '--no-renames', '--src-prefix=a/', '--dst-prefix=b/', commit, '--', '.', ...IDENT_ADD], env, false, true);
    return { commit, tree, uploadedTree: g(['write-tree'], env).trim(), overlay: crypto.createHash('sha256').update(patch || content).digest('hex'), content, groups, patch, forced, autocrlf: /^(true|input)$/.test(autocrlf) ? autocrlf : 'false', files: recs.map((r) => r.slice(r.indexOf('\t') + 1)) };  // files: the rsync path's exact list
  } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
}
// Shared tree identity (2026-10-20 lane-loop). One per process: run()'s verdict-cache lookup and runIn() used to compute
// it twice per compile job (5-25 s each of git over drvfs); the job runs the snapshot identified when it was asked for.
// Across processes: jobs started together on one snapshot (solo-loop.sh: build, gate, Windows) set
// BUILD_SLOT_IDENTITY_FILE to one fresh path. The first job computes the identity and writes it there; the others reuse
// it for the same checkout within 15 min, so every phase runs exactly that snapshot (edits made meanwhile wait for the
// next loop, as they would mid-sync). The VM still verifies the content digest.
const idMemo = new Map();
// The declaration this process identifies under: the admitted JSON, else the job's declaration file (a Windows
// snapshot runs before admission and must cover the same declared source inputs), canonicalized so both forms agree.
function declaredInputs() {
  const raw = process.env.BUILD_SLOT_INPUTS_JSON || (process.env.BUILD_SLOT_INPUTS_FILE ? fs.readFileSync(process.env.BUILD_SLOT_INPUTS_FILE, 'utf8') : '');
  return raw ? JSON.stringify(JSON.parse(raw)) : '';
}
const inputKey = () => crypto.createHash('sha256').update(declaredInputs()).digest('hex');
function sourceInputPaths(co) {
  const closure = JSON.parse(declaredInputs() || '{"inputs":[]}');
  return closure.inputs.filter(item => item.where === 'source').map(item => {
    const file = path.relative(co.top, path.resolve(process.cwd(), item.path)).split(path.sep).join('/');
    if (file.startsWith('../') || path.isAbsolute(file) || IDENT_SKIP_RE.test('\t'+file)) {
      throw new Error(`kind=INPUT_CLOSURE step=upload-custody input=${q(item.id)} owner=gate-speed recovery=${q(item.producer.map(q).join(' '))} detail='declared source input is outside the uploaded source namespace'`);
    }
    return file;
  });
}
function identityFor(co) {
  const key = co.top+'\0'+inputKey();
  const m = idMemo.get(key);
  // A commit made meanwhile (e.g. during an admission wait) moves HEAD: the git sync ships a bundle cut from the live
  // HEAD, so identify again rather than fail on "HEAD moved".
  if (m && (m.commit === 'none' || String(co.git(['rev-parse', '-q', '--verify', 'HEAD'], { encoding: 'utf8' }).stdout || '').trim() === m.commit)) return m;
  const id = identityShared(co); idMemo.set(key, id); return id;
}
function identityShared(co) {
  const f = process.env.BUILD_SLOT_IDENTITY_FILE;
  if (f) try {
    const j = JSON.parse(fs.readFileSync(f, 'utf8'));
    if (j.inputs === inputKey() && j.id.uploadedTree && j.id.overlay && j.top === co.top && Date.now() - j.at < 15 * 60e3 && (j.id.commit === 'none' || String(co.git(['rev-parse', '-q', '--verify', 'HEAD'], { encoding: 'utf8' }).stdout || '').trim() === j.id.commit)) { say(`tree identity shared from ${f}`); return { ...j.id, patch: j.id.patch == null ? null : Buffer.from(j.id.patch, 'base64') }; }
  } catch {}
  const id = treeIdentity(co);
  if (f) try {
    fs.writeFileSync(`${f}.${process.pid}`, JSON.stringify({ top: co.top, inputs: inputKey(), at: Date.now(), id: { ...id, patch: id.patch == null ? null : Buffer.from(id.patch).toString('base64') } }));
    fs.renameSync(`${f}.${process.pid}`, f);
  } catch {}
  return id;
}
// Variable-size source and launch bytes travel on stdin, never in exec argv.
// This is the sole constructor for both repository adapters.
export const MAINTAIN = '/home/var/bin/prune-wt-sdk.sh';
export function gitPrepInvocation(repo, wt, head, J, launch, payload, maintain = MAINTAIN) {
  const repoArg = repo === 'cloud' ? '' : ` ${repo}`;
  // sdk maintenance (2026-10-26, one ssh per job): its status and stderr come back as one framed BB_MAINTAIN line on
  // this connection's stdout when the script exits, whatever path it exits by.
  const maintained = repo === 'sdk' ? `bbm=$(mktemp); { ${maintain} >/dev/null 2>"$bbm.err"; echo $? > "$bbm"; } & bbmp=$!
trap 'wait $bbmp; printf "BB_MAINTAIN %s %s\\n" "$(cat "$bbm")" "$(base64 -w0 < "$bbm.err")"; rm -f "$bbm" "$bbm.err"' EXIT
` : '';
  const prepLease = repo === 'sdk' ? `mkdir -p /mnt/bb/locks/wt-sdk; exec 6>${q(`/mnt/bb/locks/wt-sdk/${path.basename(wt).replace(/\.prep$/, '')}.lock`)}; flock -s 6; ` : '';
  const input = `${maintained}${prepLease}if [ -e ${q(J + '.wpid')} ]; then echo launched; else
 o=$(printf '%s' '${payload.toString('base64')}' | base64 -d | /usr/local/bin/bb-git-prep ${q(wt)} ${head}${repoArg}); r=$?
 printf '%s\n' "$o"; [ $r = 0 ] || exit $r
 case "$o" in *prepared) ;; *) echo >&2 'bb-git-prep: exit 0 without a prepared reply'; exit 98;; esac
 printf '%s' '${zlib.gzipSync(Buffer.from(launch), { level: 6 }).toString('base64')}' | base64 -d | gzip -dc | bash
fi`;
  return { command: 'bash -s', input: Buffer.from(input) };
}

// Client snapshot (2026-10-26, admission < 5 s): on a Windows drive every git call from WSL crosses drvfs (status 7.9 s,
// add -A 5.6 s, lstat 3 ms/file). bs.ps1 runs `snapshot` here with native git.exe, which writes the tree identity, the
// merge base and the bundle for the VM-have list, and then `snapshot-attest`, which identifies the tree again. This
// client uses the snapshot only once the attestation names the same tree, before any verdict replay or launch: a tree
// changed after its identity was computed is refused (kind=TREE_IDENTITY), never shipped or replayed.
const wslPath = (p) => { const m = /^([A-Za-z]):[\\/](.*)$/.exec(p); return m ? `/mnt/${m[1].toLowerCase()}/${m[2].replace(/\\/g, '/')}` : p; };
const SNAPSHOT_KIND = 'buildbox-client-snapshot';
const attestFields = (id) => ({ commit: id.commit, tree: id.tree, uploadedTree: id.uploadedTree, content: id.content, overlay: id.overlay });
export function clientSnapshot(cwd = process.cwd()) {
  if (WIN && process.env.BUILD_SLOT_VM_HAVE_DIR) { VM_HAVE = `${process.env.BUILD_SLOT_VM_HAVE_DIR}/vm-have`; VM_HAVE_MAIN = `${process.env.BUILD_SLOT_VM_HAVE_DIR}/vm-have-main`; }
  const co = checkout(cwd); const id = treeIdentity(co);
  let have = [], bundle = null, bundleError = '';
  if (/^[0-9a-f]{40}$/.test(id.commit)) try { have = readHave(co, id.commit); if (have.length && !have.includes(id.commit)) bundle = gitBundle(co, have, id.commit); } catch (e) { bundleError = e.message.slice(0, 200); }
  return { kind: SNAPSHOT_KIND, version: 1, top: wslPath(co.top), repo: co.repo, inputs: inputKey(), at: Date.now(), base: mergeBase(co),
    have, bundle: bundle ? bundle.toString('base64') : null, bundleError, id: { ...id, patch: id.patch == null ? null : Buffer.from(id.patch).toString('base64') } };
}
export function clientAttestation(cwd = process.cwd()) {
  try { const co = checkout(cwd); return { top: wslPath(co.top), at: Date.now(), ...attestFields(treeIdentity(co)) }; }
  catch (e) { return { at: Date.now(), error: e.message.slice(0, 300) }; }
}
const snapshotRefusal = (detail) => new Error(`kind=TREE_IDENTITY step=client-snapshot owner=gate-speed recovery=${q('rerun the command through Q:/lanes/bs.ps1, which identifies the tree again')} detail=${q(detail)}`);
// Returns null without a snapshot; throws the typed refusal for any disagreement.
export async function consumeClientSnapshot(top, file = process.env.BUILD_SLOT_CLIENT_SNAPSHOT, waitMs = 60000) {
  if (!file) return null;
  const deadline = Date.now() + waitMs; let s;
  for (;;) {
    try { s = JSON.parse(fs.readFileSync(file, 'utf8')); break; }
    catch (e) { if (e.code !== 'ENOENT') throw snapshotRefusal(`unreadable snapshot ${file}: ${e.message}`); }
    if (Date.now() > deadline) throw snapshotRefusal(`no snapshot ${file} within ${waitMs / 1000}s`); await sleep(20);
  }
  if (s.kind !== SNAPSHOT_KIND || s.version !== 1) throw snapshotRefusal(`not a version-1 client snapshot: ${file}`);
  if (s.error) throw snapshotRefusal(`snapshot failed: ${s.error}`);
  if (s.top !== top) throw snapshotRefusal(`snapshot of ${s.top} offered for ${top}`);
  if (s.inputs !== inputKey()) throw snapshotRefusal('snapshot was identified under a different input declaration');
  let a;
  for (;;) { try { a = JSON.parse(fs.readFileSync(`${file}.attest`, 'utf8')); break; } catch {} if (Date.now() > deadline) throw snapshotRefusal(`no attestation ${file}.attest within ${waitMs / 1000}s`); await sleep(20); }
  if (a.error) throw snapshotRefusal(`attestation failed: ${a.error}`);
  const want = attestFields(s.id); const moved = Object.keys(want).filter((k) => want[k] !== a[k]);
  if (a.top !== s.top || a.at < s.at || moved.length) throw snapshotRefusal(`the tree changed after its identity was computed (${moved.join(', ') || 'top/time'} differ); nothing was shipped`);
  const id = { ...s.id, patch: s.id.patch == null ? null : Buffer.from(s.id.patch, 'base64') };
  return { id, base: s.base, have: s.have, bundle: s.bundle ? Buffer.from(s.bundle, 'base64') : null, bundleUsable: !s.bundleError };
}
// One ssh: prepare the worktree and launch. Returns 'launched', 'unreachable' (ssh could not connect) or 'fallback'.
// The framed maintenance result of a launch connection, in the shape maintainSourceCopies reads from its transport.
export function maintenanceResult(out) {
  const m = /^BB_MAINTAIN (\d*) ([A-Za-z0-9+/=]*)$/m.exec(out);
  return m ? { status: m[1] === '' ? 255 : Number(m[1]), stderr: Buffer.from(m[2], 'base64') } : null;
}
async function gitPrepLaunch(co, wt, launch, notes, J, id, maint = {}) {
  let sshFails = 0;
  for (let attempt = 0; attempt < 5; attempt++) {
    // The attested snapshot's have list and bundle serve the first attempt; a VM that lacks its base (97) re-reads here.
    const snap = attempt === 0 && co.snapshot?.bundleUsable ? co.snapshot : null;
    let have; try { have = snap ? snap.have : readHave(co, id.commit); } catch (e) { notes.push(`git sync unusable (${e.message.slice(0, 160)})`); return 'fallback'; }
    // No commit in common with the VM (synthetic snapshot repos, e.g. affected-tests clones): the bundle would be the whole
    // history (~11 MB); rsync's seeded delta is cheaper.
    if (!have.length) { notes.push('no history shared with the VM'); return 'fallback'; }
    let p; try { p = gitPayload(co, have, id, snap ? snap.bundle : undefined); } catch (e) { notes.push(`git sync unusable (${e.message.slice(0, 120)})`); return 'fallback'; }
    const t0 = Date.now();
    // Retry-safe: once this job's wrapper exists (an earlier attempt launched it and only its reply was lost), nothing
    // touches the worktree again. cloud's command line is unchanged; other repos name theirs.
    const invocation = gitPrepInvocation(co.repo, wt, p.head, J, launch, p.buf);
    const early = co.early; co.early = null;
    const usable = early && attempt === 0 && early.host === HOST && invocation.command === 'bash -s';
    if (early && !usable) early.cancel();
    mark('send'); PHASES.launch_bytes = invocation.input.length;
    const r = usable ? await early.send(invocation.input, 300000) : await sshRun(invocation.command, { input: invocation.input, timeout: 300000 });
    const raw = r.stdout.toString(); maint.result = maintenanceResult(raw) ?? maint.result;
    const out = raw.split('\n').filter((line) => !line.startsWith('BB_MAINTAIN ')).join('\n');
    if (r.status === 0 && /launched/.test(out)) { writeHave(co, [p.head]); notes.push(`${p.desc}, ${((Date.now() - t0) / 1000).toFixed(1)}s${sshFails ? ` after ${sshFails} ssh retries` : ''}`); return 'launched'; }
    if (r.status === 97) { writeHave(co, out.split('\n').filter((s) => /^[0-9a-f]{40}$/.test(s)), true); notes.push('VM lacked the bundle base; rebundled'); continue; }
    // ssh could not connect. A congested uplink (banner/handshake timeouts) is not an outage: retry here rather than
    // falling back to Daytona, whose multi-MB source upload would congest it further. Refused/no route: unreachable.
    if (r.status === 255 && !out) {
      const e = r.stderr.trim().split('\n').pop()?.slice(0, 120) || '';
      if (/banner exchange|timed out|Connection reset|closed by remote/i.test(e) && !/refused|No route/i.test(e) && ++sshFails < 4) { await sleep(3000); attempt--; continue; }
      notes.push(`ssh: ${e}`); return 'unreachable';
    }
    if (r.status === 96) return 'unreachable';
    notes.push(`git prep rc ${r.status} (${r.stderr.trim().split('\n').pop()?.slice(0, 120) || ''}); rsync`); return 'fallback';
  }
  return 'fallback';
}

// Cargo trusts mtimes: a target dir must only ever see one source tree, whose changed files always get a fresh mtime
// (git checkout/apply write now; rsync runs without -t). Shared between trees, a
// tree last written before another tree's build would look fresh with different content (stale binary, false red).
const COLD = process.env.BUILD_SLOT_REMOTE_COLD === '1';
const tdir = (src, tkey) => `/mnt/bb/targets/${tkey}/${path.basename(path.dirname(src))}-${path.basename(src)}`;

// Non-cargo jobs see a host-shaped target dir (scripts check it, e.g. affected-tests.mjs: /home/var/targets/<lane>); their
// cargo steps go through the VM's /home/var/bin/build-slot, which maps it under /work/target/nested (per source tree).
const hostTarget = (src) => { const t = process.env.CARGO_TARGET_DIR || '';
  return /^\/home\/var\/targets\/[A-Za-z0-9_-]+$/.test(t) ? t : `/home/var/targets/${path.basename(src).replace(/\.\d+$/, '').replace(/[^A-Za-z0-9_-]/g, '-')}`; };
// Seed an empty target dir T from a warm one (see /home/var/opt/buildbox/bb-incr-patch.mjs). Runs in the job wrapper.
function seedScript(T, tkey, repo) {
  const repoGlob = repo === 'cloud' ? '/mnt/bb/targets/*/*' : `/mnt/bb/targets/${REPOS[repo].tkey}*/*`;
  return `T=${T}
# Disk gate (22:15Z /mnt/bb hit 100%, mostly seeded per-slot copies): no seed at 70% full or more.
if [ -z "$(ls -A $T 2>/dev/null)" ] && [ "$(df --output=pcent /mnt/bb | tail -1 | tr -dc 0-9)" -lt 70 ]; then
  # Candidates: the 3 newest targets of this target key, then the newest of the repo's 20 most recent real builds (over
  # 3000 artifacts in debug/deps). Up to 4 tries, at most 15 GB each, 10 s for each one's cargo lock.
  tries=0; s0=$(date +%s)
  for c in $( { ls -dt /mnt/bb/targets/${tkey}/*/debug/.fingerprint 2>/dev/null | head -3
               for r in $(ls -dt ${repoGlob}/debug/.fingerprint 2>/dev/null | ${repo === 'cloud' ? "grep -v '^/mnt/bb/targets/\\(sdk\\|cold\\)/' |" : ''} head -20); do
                 [ "$(ls \${r%/.fingerprint}/deps 2>/dev/null | wc -l)" -gt 3000 ] && echo $r; done; } | grep -v "^$T/" | awk '!seen[$0]++'); do
    [ $tries -lt 4 ] || break; SEED=\${c%/debug/.fingerprint}
    [ "$(du -s --block-size=1G "$SEED" 2>/dev/null | cut -f1)" -le 15 ] || continue
    tries=$((tries + 1)); mkdir -p $T.seed.$$
    # The seed's own unit lock, shared (2026-10-21 unitlock): no eviction renames it away mid-copy.
    SL=/mnt/bb/locks/unit/$(echo "\${SEED#/mnt/bb/targets/}" | cut -d/ -f1,2 | tr / %).lock
    if flock -s -w 10 "$SL" flock -w 10 "$SEED/debug/.cargo-lock" cp -a --reflink=auto "$SEED/." $T.seed.$$/ 2>/dev/null; then
      find $T.seed.$$ -path '*/.fingerprint/*' -type f -exec touch -h -d @1 {} + 2>/dev/null
      rmdir $T 2>/dev/null; mv -T $T.seed.$$ $T 2>/dev/null &&
        echo "[buildbox] target seeded from $SEED ($(du -s --block-size=1G $T | cut -f1) GB, $(( $(date +%s) - s0 ))s, try $tries)" >> $J.err
      rm -rf $T.seed.$$; break
    fi
    rm -rf $T.seed.$$
  done
fi
`;
}
// Shared warm target dirs (2026-10-16 compute reduction; /mnt/bb held 981 per-lane-copy target dirs, each cold the first
// time a lane copy ran): a cargo job builds in one of 16 slots /mnt/bb/targets/shared-<repo>-<flavor>/<n> (bb-target-prune: one LRU unit per slot; a running job's slot is the $J.tdir it names), the first one
// whose lock is free (the job wrapper holds it until the job ends); flavor is the toolchain override plus the forwarded
// RUSTFLAGS-like env (what makes cargo rebuild rather than keep both builds); features coexist in one dir. Cargo trusts
// mtimes, so before building, every source file gets the time its content first entered that slot: <slot>/.bb-mtimes.json
// maps path -> [blob, time]; an unchanged blob keeps its recorded time, a changed or new one gets now (newer than anything
// built from the old content). The tree-identity records are the file list, so this covers exactly the tracked and
// untracked non-ignored files. Non-cargo jobs (their host-shaped targets, /home/var/src copies) keep the per-copy dir, as
// do --cold jobs and BUILD_SLOT_SHARED_TARGET=0. All 16 slots busy: the per-copy dir.
function sharedFlavor(argv, repo) {
  if (COLD || path.basename(argv[0] || '') !== 'cargo' || process.env.BUILD_SLOT_SHARED_TARGET === '0') return null;
  const tc = (argv[1] || '').startsWith('+') ? argv[1].slice(1) : 'default';
  const env = passEnv().split('\n').filter((l) => /^export (RUSTFLAGS|RUSTDOCFLAGS|CARGO_ENCODED_RUSTFLAGS|CARGO_ENCODED_RUSTDOCFLAGS|CARGO_PROFILE_[A-Z_]+|CARGO_BUILD_[A-Z_]+)=/.test(l)).sort().join('\n');
  return `${repo}-${tc.replace(/[^A-Za-z0-9._-]/g, '_')}-${crypto.createHash('sha256').update(env).digest('hex').slice(0, 10)}`;
}
// argv[1] recs file (NUL-separated "<blob> <stage>\t<path>"), argv[2] the slot's map, argv[3] the source root.
const MTIME_MAP = `const fs = require('fs'); const [recsF, mapF, root] = process.argv.slice(1);
let map = {}; try { map = JSON.parse(fs.readFileSync(mapF, 'utf8')); } catch {}
const now = Date.now() / 1000; const next = {}; let kept = 0, fresh = 0;
for (const r of fs.readFileSync(recsF, 'utf8').split('\\0')) { if (!r) continue; const t = r.indexOf('\\t'); const blob = r.slice(0, r.indexOf(' ')); const p = r.slice(t + 1);
  const prev = map[p]; let m; if (prev && prev[0] === blob) { m = prev[1]; kept++; } else { m = now; fresh++; }
  try { fs.lutimesSync(root + '/' + p, m, m); next[p] = [blob, m]; } catch (e) { if (e.code !== 'ENOENT') throw e; } }
fs.writeFileSync(mapF + '.tmp', JSON.stringify(next)); fs.renameSync(mapF + '.tmp', mapF);
console.error('[buildbox] shared target ' + mapF.replace(/\\/[^/]*$/, '') + ': ' + kept + ' files keep their times, ' + fresh + ' changed or new');`;
// A private target is seeded only by the exact uploaded content key. Give that
// immutable input stable, past times before any compiler runs, so a new source
// checkout does not invalidate its completed target snapshot. This must never
// apply to a mutable target shared across source trees: hash-derived times are
// not monotonic. Later writes in this job retain their actual fresh mtimes.
const SOURCE_TIMES = `const fs=require('fs'),crypto=require('crypto');const [records,root,clock,content]=process.argv.slice(1);
const files=[],names=[],directories=new Map();let cacheable=true;
for(const record of fs.readFileSync(records,'utf8').split('\\0')){
 if(!record)continue;
 const blob=record.slice(0,record.indexOf(' ')),name=record.slice(record.indexOf('\\t')+1);
 if(!/^[a-f0-9]{40,64}$/.test(blob)||!name||name.startsWith('/')||name.split('/').includes('..'))throw Error('invalid uploaded source record');
 const seconds=946684800+parseInt(blob.slice(0,8),16)%(20*365*86400);
 fs.lutimesSync(root+'/'+name,seconds,seconds);
 names.push(name);
 const parts=name.split('/');
 for(let count=0;count<parts.length;count++){
  const directory=parts.slice(0,count).join('/');
  if(!directories.has(directory))directories.set(directory,crypto.createHash('sha256'));
  directories.get(directory).update(blob+'\\t'+parts.slice(count).join('/')+'\\0');
 }
}
// Cargo build scripts may watch directories (proto/data), not just files.
// Fresh checkout directory times would otherwise invalidate a complete target.
// Only this exact private uploaded tree receives deterministic past times.
for(const [name,hash] of directories){
 const directory=root+(name?'/'+name:''),real=fs.realpathSync(directory);
 if((real!==root&&!real.startsWith(root+'/'))||!fs.lstatSync(directory).isDirectory()){cacheable=false;continue;}
 const seconds=946684800+parseInt(hash.digest('hex').slice(0,8),16)%(20*365*86400);
 fs.utimesSync(directory,seconds,seconds);
}
// Normalize every target before inspecting symlink aliases to those targets.
for(const name of names){
 try{
  const real=fs.realpathSync(root+'/'+name);
  if(!real.startsWith(root+'/')){cacheable=false;continue;}
  const s=fs.statSync(root+'/'+name,{bigint:true});
  files.push([name,...[s.mode,s.size,s.mtimeNs,s.ctimeNs].map(String),crypto.createHash('sha256').update(fs.readFileSync(real)).digest('hex')]);
 }catch{cacheable=false;}
}
fs.writeFileSync(clock+'.tmp',JSON.stringify({root,content_sha256:content,cacheable,files,
 namespace:{version:1,pathspec:${JSON.stringify(IDENT_ADD)},skip:${JSON.stringify(IDENT_SKIP)}}}));fs.renameSync(clock+'.tmp',clock);
console.error('[buildbox] source clock: exact-content private targets');`;
// node_modules for every job that is not cargo (2026-10-18 crit-bslot): remote trees often had none and JS gates died
// rc 127 ("./node_modules/.bin/tsc: No such file or directory", #5338, #5341) when the lane forgot `npm ci`. Every tracked
// package-lock.json (managed-agents excluded) gets `npm ci` through the bbnpm shim before the command: a no-op when the
// tree's node_modules already matches the lockfile, else a reflink copy from the per-lockfile-hash store /mnt/bb/nm, else a
// real ci that publishes there. The invocation forms (`npm ci` at the root, `npm ci --prefix <dir>`) match what lanes run,
// so store hashes are shared with them. A failure is reported and the command still runs. BUILD_SLOT_NO_NPM=1 skips.
const NPM_ENSURE = "if [ -z \"${BUILD_SLOT_NO_NPM:-}\" ]; then (cd /work/src && bbnt=$EPOCHREALTIME && bbnl=$(mktemp /tmp/bbnpm.XXXXXX) &&\n  for f in $(git ls-files -- package-lock.json '*/package-lock.json' 2>/dev/null | grep -v '^managed-agents/'); do\n    d=$(dirname \"$f\"); o=$bbnl.$(echo \"$d\" | tr / _)\n    { if [ \"$d\" = . ]; then npm ci; else npm ci --prefix \"$d\"; fi || echo \"rc=$?\"; } > \"$o\" 2>&1 &\n  done; wait\n  for l in \"$bbnl\".*; do [ -e \"$l\" ] || continue; grep -q '^rc=' \"$l\" && { echo \"[buildbox] node_modules: npm ci for ${l##*.} failed (the command still runs):\"; tail -5 \"$l\"; }; done >&2\n  rm -f \"$bbnl\" \"$bbnl\".*; echo \"[buildbox] node_modules ensured for tracked lockfiles in $(awk -v a=$EPOCHREALTIME -v b=$bbnt 'BEGIN{printf \"%.1f\", a-b}')s\" >&2)\nfi\n";
// bb-admit owns admission and Cargo parallelism on the build box.
let jobTdir = '';  // the target dir the job actually used (its wrapper writes $J.tdir), for copy-back
function launchScript(src, tkey, rel, argv, J, pathPre = '', repo = 'cloud', base = '', id, maxSec = jobMaxSec(), idleOk = process.env.BUILD_SLOT_IDLE_OK === '1', receipt) {
  const tenant = tenantOf(process.env.BUILD_SLOT_LABEL || path.basename(src), process.cwd(), repo, argv);
  const watcherRefusal = 'kind=WATCHER step=watcher-retirement owner=gate-speed recovery=' + q('BUILD_SLOT_TENANT='+tenant+' /home/var/bin/build-slot '+argv.map(q).join(' '));
  // Compiler policy belongs to admission_policy.py, including explicit
  // BUILD_SLOT_COMPILER=1 script producers. The transport must not overwrite
  // or clear its admitted environment after entering bb-job.
  const cargoJob = path.basename(argv[0]) === 'cargo';
  const flavor = ''; const TD = `${J}.targets`; const SH = '';
  const jobHostTarget = `${TD}/.host-targets/${path.basename(hostTarget(src))}`;
  const exportSpec = { prefix: J, cwd: `/work/src/${rel}`, paths: (process.env.BUILD_SLOT_REMOTE_FETCH || '').split(/\s+/).filter(Boolean), build: cargoJob,
    targetMapping: glibcPin ? { triple: GLIBC_TRIPLE, profiles: [...glibcPin.dirs] } : null };
  const leaseFile = `/mnt/bb/locks/wt-sdk/${path.basename(src).replace(/\.prep$/, '')}.lock`;
  // /home/var/bin/bbnpm/npm: \`npm ci\` is skipped when node_modules was installed from the same lockfile hash (node_modules
  // persists per copy and seeds new copies; rsync never carries it).
  let inner = `set -u
${COLD ? 'export BB_CACHE_DISABLED=1 BB_TARGET_STORE_DISABLED=1\n' : ''}export BUILD_SLOT_RECEIPT_META=${J}.receipt-meta.v2.json BUILD_SLOT_EXEC_IN_PAYLOAD=1
export BB_JOB_GIT_SOURCE=/work/src BB_JOB_PRIVATE_MOUNT=1
export BB_JOB_EXPORT_SPEC=${q(JSON.stringify(exportSpec))}
if [ "\${BB_JOB_ACTIVE:-}" != 1 ]; then exec python3 /home/var/bin/bb-job.py bash ${J}.sh; fi
# The host gates run under umask 022; bb1 logins default to 002, which made job-created dirs group-writable (custody checks
# such as cloud-supervisor release_cache refuse those).
umask 022
export HOME="$BB_JOB_HOME" CARGO_HOME=/mnt/bb/toolchains/cargo RUSTUP_HOME=/home/var/.rustup PATH=/home/var/bin/bb-cargo:${pathPre}/home/var/bin/bbnpm:/home/var/.cargo/bin:/home/var/tools/node24/node-v24.3.0-linux-x64/bin:/home/var/bin:/usr/local/bin:/usr/bin:/bin
# The job's delegated cgroup (see the launch below): BB_CGROUP is the job leaf; the bbcg systemd-run shim turns
# "systemd-run --user --scope -p Delegate=yes -p MemoryMax=N CMD" into an exclusive child cgroup inside the job.
export BB_CGROUP=$(python3 /home/var/bin/cpu_leases.py work) PATH=/home/var/bin/bbcg:$PATH
# Host paths behind this job's binds of /tmp, /work/src and /work/target (docker -v resolves on the host).
export BB_HOST_TMP=${J}.tmp BB_HOST_SRC=${src} BB_HOST_TARGET=$(cat ${J}.tdir 2>/dev/null || echo ${TD})
export CARGO_INCREMENTAL=0  # job-owned targets are deleted; use shared sccache instead
export TMPDIR="$BB_JOB_TMP" CARGO_TARGET_DIR="$BB_JOB_TARGET" CARGO_PROFILE_DEV_INCREMENTAL=false CARGO_PROFILE_TEST_INCREMENTAL=false CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \\
  CARGO_NET_RETRY=5 CARGO_TERM_COLOR=\${CARGO_TERM_COLOR:-never}
printf '[profile.default]\\nslow-timeout = { period = "60s", terminate-after = 10 }\\n' > /tmp/bb-nextest.toml  # = NEXTEST_TOOL, literal: bb-swap smokes this block verbatim
${testRunner()}
# Tests that read the checkout's git HEAD need a repository whose HEAD is the tree: a git-synced worktree is at the client's
# commit already; uncommitted changes (and rsynced trees) are committed on top (ignored files stay out).
cd /work/src && bbpre=$(git rev-parse -q --verify HEAD 2>/dev/null) || bbpre=
cd /work/src && { [ -e .git ] || git init -q; } && { [ -f .git ] || rm -f .git/index.lock; } &&
  { git rev-parse -q --verify HEAD >/dev/null && [ -z "$(git status --porcelain 2>/dev/null | head -1)" ] ||
    { git add -A >/dev/null 2>&1 && git -c user.name=bb -c user.email=bb@localhost commit -q --no-verify --allow-empty -m buildbox >/dev/null 2>&1; }; } ||
  echo >&2 "[buildbox] note: could not commit the synced tree (tests reading git HEAD may differ)"
# origin/main with history shared with HEAD (scripts diff origin/main...HEAD, e.g. ops/ci/affected-tests.mjs).
BBM=/home/var/mirror/${repo}.git BBB=${base}
if [ -f .git ]; then  # worktree of the mirror: refs are the mirror's
  git rev-parse -q --verify refs/remotes/origin/main >/dev/null || git symbolic-ref refs/remotes/origin/main refs/heads/main 2>/dev/null
elif [ -d .git ] && [ -d $BBM ]; then  # rsync tree: borrow the mirror's objects; hang the tree on the client's merge-base
  grep -qxF $BBM/objects .git/objects/info/alternates 2>/dev/null || echo $BBM/objects >> .git/objects/info/alternates
  git update-ref refs/remotes/origin/main "$(git --git-dir=$BBM rev-parse refs/heads/main)"
  { [ -n "$BBB" ] && git cat-file -e "$BBB^{commit}" 2>/dev/null; } || { [ -n "$BBB" ] && echo >&2 "[buildbox] note: merge-base $BBB not in the VM mirror; basing on main"; BBB=$(git rev-parse refs/remotes/origin/main); }
  if [ "$(git rev-parse -q --verify HEAD^ 2>/dev/null)" != "$BBB" ] || [ "$(git rev-list --count HEAD 2>/dev/null)" = 1 ]; then
    c=$(git -c user.name=bb -c user.email=bb@localhost commit-tree "HEAD^{tree}" -p "$BBB" -m buildbox) && git update-ref HEAD "$c"; fi
fi
if [ ${repo} = cloud ] && ! { git rev-parse -q --verify refs/remotes/origin/main >/dev/null && git merge-base refs/remotes/origin/main HEAD >/dev/null 2>&1; }; then
  echo >&2 "[build-slot] remote failed: no-base-ref (origin/main missing or unrelated to HEAD in the synced checkout)"; exit 224; fi
${cargoJob ? '' : NPM_ENSURE}
# Tree identity (2026-10-16 treeid): the content this job runs on must be exactly the client's. Same digest as treeIdentity().
# A throwaway index seeded from the client's commit without stat data, so every file is hashed again under the client's
# core.autocrlf (an entry the commit block cached was hashed without it), while committed CRLF blobs stay as committed
# (git never converts a file whose index blob has CR), as on the client.
cd /work/src && bbI=$(mktemp -u /tmp/bbid.XXXXXX) && bbid=$(I=$bbI && export GIT_INDEX_FILE=$I &&
  { [ ${id.commit} = none ] || git read-tree ${id.commit} 2>/dev/null || rm -f $I; } && git -c core.autocrlf=${id.autocrlf} -c core.safecrlf=false add -A -- . ${IDENT_ADD.map(q).join(' ')} >/dev/null 2>&1 &&${id.forced.length ? `
  { git -c core.autocrlf=${id.autocrlf} -c core.safecrlf=false add -f -- ${id.forced.map(q).join(' ')} >/dev/null 2>&1 || true; } &&` : ''}
  git ls-files -s -z | grep -zvE ${q(IDENT_SKIP)} | sed -z 's/^[0-7]* //' > $I.recs && sha256sum < $I.recs | cut -c1-64; rm -f $I $I.lock)
echo >&2 "BUILD_SLOT_TREE=${id.commit} ${id.tree} \${bbid:-unknown}"
# HEAD: the client commit, or the commit block's "buildbox" commit on it (bb-git-prep leaves an unchanged sdk tree as is).
if [ "\${bbid:-}" != ${id.content} ] || { [ -f .git ] && [ ${id.commit} != none ] && [ "$bbpre" != ${id.commit} ] &&
  ! { [ "$(git rev-parse -q --verify "$bbpre^" 2>/dev/null)" = ${id.commit} ] && [ "$(git log -1 --format=%s "$bbpre" 2>/dev/null)" = buildbox ]; }; }; then
  echo >&2 "[build-slot] remote failed: tree-mismatch (client commit ${id.commit} content ${id.content}; VM checkout HEAD \${bbpre:-none} content \${bbid:-unknown})"
  [ ! -s "$bbI.recs" ] || node -e ${q(IDENT_DIFF)} "$bbI.recs" ${id.groups} || true; rm -f "$bbI.recs"; exit 224; fi
# Shared target slot (see sharedFlavor): source times from the slot's content map before cargo looks at them.
if [ ${repo} = sdk ] && [ "$BB_HOST_TARGET" = ${TD} ]; then
  if ! node -e ${q(SOURCE_TIMES)} "$bbI.recs" /work/src ${J}.source-clock.json ${id.content}; then
    echo >&2 '[build-slot] remote failed: private-source-times'; rm -f "$bbI.recs"; exit 224; fi
  export BB_SOURCE_TIMES_POLICY=blob-past-private-v1 BB_SOURCE_TIMES_FILE=${J}.source-clock.json
fi
if [ "$BB_HOST_TARGET" != ${TD} ] && ! node -e ${q(MTIME_MAP)} "$bbI.recs" "$BB_HOST_TARGET/.bb-mtimes.json" /work/src; then
  echo >&2 "[build-slot] remote failed: shared-target-mtimes (could not set source times for $BB_HOST_TARGET; BUILD_SLOT_SHARED_TARGET=0 uses a per-copy dir)"; rm -f "$bbI.recs"; exit 224; fi
rm -f "$bbI.recs"
export BUILD_SLOT_TREE="${id.commit} ${id.tree} ${id.content}"
export BUILD_SLOT_RECEIPT_META=${J}.receipt-meta.v2.json
cd /work/src/${q(rel)} && node ${J}.receipt-tool.mjs exec ${J}.receipt-meta.v2.json && ${argv.map((a, i) => q(i === 0 && path.basename(a) === 'cargo' ? '/home/var/bin/bb-cargo/cargo' : a)).join(' ')}`;
  // Hash the complete generated build environment/driver before adding the
  // digest itself. Dispatch filenames are operational, not compiler inputs.
  const targetRequestHash = crypto.createHash('sha256').update(inner.split(J).join('$JOBPREFIX')).digest('hex');
  inner = `export BB_TARGET_REQUEST_SHA256=${targetRequestHash}\n` + inner;
  let wrapper = `J=${J}
export PATH=/home/var/tools/node24/node-v24.3.0-linux-x64/bin:/home/var/.cargo/bin:/usr/local/bin:/usr/bin:/bin
BB_CALLER_RLIMITS_JSON=$(python3 /home/var/bin/admission_policy.py capture-limits) || exit 224
export BB_CALLER_RLIMITS_JSON BB_CALLER_RLIMITS_READY=1
node $J.receipt-tool.mjs arm $J.receipt-meta.v2.json $$
receipt_done() {
  rc=$?; trap - EXIT
  category=command
  [ -f $J.receipt-meta.v2.json.exec ] || category=infra
  [ $rc -lt 128 ] || category=interrupted
  case $rc in 222|224) category=infra;; esac
  [ ! -f $J.interrupted ] || category=interrupted
  [ ! -f $J.cancelled ] || category=cancelled
  rm -rf -- ${TD}
  node $J.receipt-tool.mjs finish $J.receipt-meta.v2.json $rc $category >> $J.out 2>> $J.err
  finished=$?; [ $finished = 0 ] || rc=$finished
  echo $rc > $J.rc.tmp && mv $J.rc.tmp $J.rc
}
trap receipt_done EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
mkdir -p /mnt/bb/locks/wt-sdk
exec 6>${leaseFile}
flock -s 6
echo $$ > $J.wpid
printf '%s\\n' BB-WRAPPER-READY >&3
exec 3>&-
while [ "$(awk '/MemAvailable/{print int($2/1048576)}' /proc/meminfo)" -lt 24 ] || [ "$(awk -F'[= ]' '/^some/{print int($3)}' /proc/pressure/memory 2>/dev/null || echo 0)" -gt 10 ]; do sleep 5; done
# Warm restore (2026-10-18, crit-bslot): after a fresh /mnt/bb, bb-warm restore copies sccache back from the persistent disk
# /mnt/bbp first; jobs wait for it (max 10 min) so no sccache server starts on a half-copied dir.
[ -n "$(find /mnt/bb/.warm-restoring -mmin -15 2>/dev/null)" ] && echo "[buildbox] waiting for the warm-cache restore (bb-warm restore: sccache from the persistent disk after a fresh /mnt/bb; max 10 min)" >> $J.err
while [ -n "$(find /mnt/bb/.warm-restoring -mmin -15 2>/dev/null)" ] && [ $SECONDS -lt 600 ]; do sleep 5; done
# Free-space floor (2026-10-20 lane-loop; /mnt/bb hit 100% at 11:30Z): under 150 GB free, LRU idle target dirs go until
# 300 GB are free (/home/var/bin/bb-free-space; never a target a live job holds).
[ -x /home/var/bin/bb-free-space ] && /home/var/bin/bb-free-space $J >> $J.err 2>&1
T=${TD}
${SH ? `for i in $(seq 0 15); do mkdir -p ${SH}/$i && exec 8>${SH}/$i.lock && flock -n 8 && { T=${SH}/$i; break; }; exec 8>&-; done  # held until the wrapper exits
` : ''}# Unit locks (2026-10-21 unitlock): at 18:40:58Z bb-disk-sweep's size cap evicted targets/sdk/pr-gate-d269b8dd (31 GB) 44 s
# after a gate job started in it: the sweep's live-job snapshot predated the wrap, the dir was 30+ min old after downtime,
# and cargo (blocked on the package cache) held no .cargo-lock yet. The wrapper now holds a SHARED flock on
# /mnt/bb/locks/unit/<a>.lock and <a>%<b>.lock for targets/<a>/<b> of $T and of the per-copy dir, for its whole life, taken
# before anything touches them (the fds stay open; children may inherit them). Every eviction (bb-disk-sweep,
# bb-target-prune, bb-free-space) takes the same files EXCLUSIVE with flock -n, skips a held unit and renames under it,
# so after the lock a unit is whole or absent; mkdir -p then recreates an absent one. bb-free-space above runs first.
mkdir -p /mnt/bb/locks/unit
for bbu in $(for p in $T ${TD}; do r=\${p#/mnt/bb/targets/}; echo "\${r%%/*}"; echo "$r" | cut -d/ -f1,2 | tr / %; done | sort -u); do
  : >> /mnt/bb/locks/unit/$bbu.lock 2>/dev/null; { exec {bbfd}</mnt/bb/locks/unit/$bbu.lock; } 2>/dev/null && flock -s -w 1800 $bbfd ||
    { echo "[build-slot] remote failed: unit-lock (no shared lock on /mnt/bb/locks/unit/$bbu.lock within 30 min)" >> $J.err
      rm -f $J.sh; exit 224; }
done
mkdir -p $T ${TD}
echo $T > $J.tdir.tmp && mv $J.tdir.tmp $J.tdir
# Zero-byte sweep (2026-10-18, crit-bslot; host /home/var/bin/cargo-zero-sweep): a crash can leave empty .rlib/.rmeta files that
# cargo calls fresh (E0463 one crate at a time). Drop empty/truncated deps artifacts and their same-hash fingerprints.
find $T ${TD}/.host-targets -maxdepth 4 -path '*/deps/*' -type f \\( -name '*.rlib' -o -name '*.rmeta' -o -name '*.so' \\) -size -16c -print0 2>/dev/null |
  while IFS= read -r -d '' f; do b=$(basename "$f"); h=$(echo "$b" | sed -E 's/.*-([0-9a-f]+)\\.[a-z]+$/\\1/'); rm -f "$f"
    echo "[buildbox] zero-sweep: removed empty/truncated $b" >> $J.err
    case "$h" in *[!0-9a-f]*|'') ;; *) rm -rf "$(dirname "$(dirname "$f")")/.fingerprint/"*-"$h";; esac; done
mkdir -p $J.tmp $J.work/src $J.work/target && chmod 1777 $J.tmp  # this job's private /tmp and /work (on /mnt/bb)
# Host-shaped target dirs (/home/var/targets -> /mnt/bb/home/targets) are per source tree too (2026-10-16 treeid): shared by
# label, a cargo call in a script built one tree, then another tree whose files were synced earlier (older mtimes) looked fresh
# and ran the first tree's test binary. Created after the seed (the seed only fills an empty target dir).
mkdir -p ${TD}/.host-targets/${path.basename(hostTarget(src))} ${TD}/.host-src
# The host-shaped source dirs (/home/var/src -> /mnt/bb/home/src: affected-tests copies its tree there) are per source tree
# as well, and every ancestor of a job path is writable by its owner only, as on the host (2026-10-16 custody: /mnt/bb/home
# was 0775, so the cloud-supervisor ancestor custody check refused every fixture under /home/var/src on bb1 only).
chmod go-w ${SH ? `/mnt/bb/targets ${SH} ` : ''}$T /mnt/bb /mnt/bb/home /mnt/bb/home/src /mnt/bb/home/targets ${TD} ${TD}/.host-targets ${TD}/.host-src 2>/dev/null || true
# /work itself is the job's own directory on /mnt/bb (2026-10-26: siblings of the /work/target bind, such as
# check-feature-matrix's <target>-features-N, landed on the shared root filesystem and filled it).
# Each job is its own Delegate=yes scope (2026-10-16 delegated cgroup): root moves the job into a "job" leaf and hands the
# scope to var, so jobs (local e2e's heap cgroups, via /home/var/bin/bbcg/systemd-run) can build cgroups inside the job.
# Only a cgroup that is a bbjobs.slice scope is ever touched.
setsid sudo -n --preserve-env=BB_CALLER_RLIMITS_JSON unshare -m --propagation private bash -c 'mount --bind ${J}.work /work && mount --bind ${src} /work/src && mount --bind '"$T"' /work/target && mount --bind ${TD}/.host-targets /mnt/bb/home/targets && mount --bind ${TD}/.host-src /mnt/bb/home/src && mount --bind ${J}.tmp /tmp && exec python3 /home/var/bin/admission_policy.py exec-user var -- bash ${J}.sh' >> $J.out 2>> $J.err < /dev/null 8>&- &
p=$!; echo $p > $J.pid
node $J.receipt-tool.mjs job $J.receipt-meta.v2.json $p
# Hang reaper (2026-10-18, crit-bslot): the job's scope cgroup must use 5 s of CPU per 10 min; a job under that (a deadlocked
# test, a wedged wait) is killed (rc 124) so its admission slot frees. BUILD_SLOT_IDLE_OK=1 exempts a job that legitimately
# waits (live cloud ops). Sampled every 30 s over a sliding 20-sample window; jobs without a bbjobs scope are not reaped.
p_birth=$(python3 /home/var/bin/admission_policy.py process-identity "$p")
( exec 8>&-; s0=$(date +%s); cg=; n=0; declare -a u
  while :; do
    python3 /home/var/bin/admission_policy.py watch-wait "$p" "$p_birth" 30; sampled=$?
    case "$sampled" in 1) break;; 0) :;; *) echo ${q(watcherRefusal)} >> $J.err; exit 224;; esac
    if [ -z "$cg" ]; then c=$(pgrep -P $p | head -1); [ -n "$c" ] && cg=$(sed -n 's/^0:://p' /proc/$c/cgroup 2>/dev/null)
      case "$cg" in /bbjobs.slice/buildbox-cpu/slot-*/run-*/work/job) cg=/sys/fs/cgroup\${cg%/work/job};; *) [ -n "$c" ] && cg=- || cg=;; esac; fi
    if ${idleOk ? 'false' : 'true'} && [ -n "$cg" ] && [ "$cg" != - ] && [ -r $cg/cpu.stat ]; then n=$((n + 1)); u[$n]=$(awk '/^usage_usec/{print $2}' $cg/cpu.stat)
      if [ $n -gt 20 ] && [ $(( u[n] - u[n-20] )) -lt 5000000 ]; then touch $J.idle
        echo "[buildbox] hang reaper: the job used $(( (u[n] - u[n-20]) / 1000 )) ms of CPU in the last 10 min (minimum 5 s): presumed hung (deadlocked test or stuck wait); killed (rc 124), admission slot freed. BUILD_SLOT_IDLE_OK=1 exempts a job that legitimately waits" >> $J.err
        kill -TERM -- -$p 2>/dev/null; python3 /home/var/bin/admission_policy.py watch-wait "$p" "$p_birth" 10 && kill -KILL -- -$p 2>/dev/null; break; fi; fi
    if [ $(( $(date +%s) - $(stat -c %Y $J.hb) )) -gt 900 ]; then touch $J.interrupted; echo "[buildbox] client heartbeat lost for 15 min: killing the job" >> $J.err
      kill -TERM -- -$p 2>/dev/null; python3 /home/var/bin/admission_policy.py watch-wait "$p" "$p_birth" 10 && kill -KILL -- -$p 2>/dev/null; break; fi
    if [ $(( $(date +%s) - s0 )) -gt ${maxSec} ]; then touch $J.ceiling
      echo "[buildbox] job ceiling: still running after ${Math.round(maxSec / 60)} min; killed (rc 124). Lane jobs get 30 min; merge gates, qualification (BUILD_SLOT_TIER=qual) and BUILD_SLOT_STRESS=1 runs 3 h" >> $J.err
      kill -TERM -- -$p 2>/dev/null; python3 /home/var/bin/admission_policy.py watch-wait "$p" "$p_birth" 10 && kill -KILL -- -$p 2>/dev/null; break; fi; done ) &
watcher=$!
wait $p; rc=$?
wait "$watcher" || { echo ${q(watcherRefusal)} >> $J.err; rc=224; }
[ -e $J.ceiling ] && rc=124; [ -e $J.idle ] && rc=124
# Last use for bb-target-prune's LRU (2026-10-20 lane-loop): it reads the unit dir's own mtime, which builds inside
# .host-targets/ never change, so an inner-loop target used minutes ago was evicted as the oldest (solo-hib, lane-loop).
touch ${TD} $T 2>/dev/null || true
rm -f $J.sh
rm -rf -- ${TD}
rm -rf $J.tmp $J.work 2>/dev/null || sudo -n rm -rf $J.tmp $J.work
exit $rc`;
  const boundary = wrapper.indexOf('while [ "$(awk');
  if (boundary < 0) throw new Error('missing remote admission boundary');
  const admissionSave = q("import {atomic} from '/home/var/bin/buildbox-receipt.mjs';atomic(process.argv[1],{tier:process.env.BB_ADMIT_TIER,tenant:process.env.BB_ADMIT_TENANT,wait_seconds:Number(process.env.BB_ADMIT_WAIT_SECONDS),build_jobs:Number(process.env.CARGO_BUILD_JOBS)});");
  const admitted = 'J=' + J + '\nexport PATH=/home/var/tools/node24/node-v24.3.0-linux-x64/bin:/home/var/.cargo/bin:/home/var/bin:/usr/bin:/bin\nnode --input-type=module -e ' + admissionSave + ' ' + J + '.receipt-meta.v2.json.admission\n' + wrapper.slice(boundary);
  const cls = jobClass(argv) || 'light';
  wrapper = wrapper.slice(0, boundary) + `export BUILD_SLOT_RECEIPT_META=${J}.receipt-meta.v2.json BB_ADMIT_CLASS=${q(cls)} BB_ADMIT_WRAPPED_COMMAND=1
${passEnv()}
${COLD ? 'export BB_CACHE_DISABLED=1 BB_TARGET_STORE_DISABLED=1\n' : ''}
export BUILD_SLOT_INPUTS_JSON=${q(process.env.BUILD_SLOT_INPUTS_JSON || '')} BUILD_SLOT_INPUT_BASE=${q(process.env.BUILD_SLOT_INPUT_BASE || base || 'none')} BUILD_SLOT_INPUT_ROOT=${q(path.posix.join(src, rel))}
/home/var/bin/bb-admit ${jobTier} ${q(tenant)} -- bash ${J}.admitted >> ${J}.out 2>> ${J}.err
exit $?`;
  return `set -eu
J=${J}; mkdir -p /mnt/bb/jobs ${TD} ${src} /mnt/bb/home/src /mnt/bb/home/targets
[ ! -e $J.wpid ] || { echo launched; exit 0; }  # idempotent: a retried launch never starts a second job
echo ${Buffer.from(RECEIPT_TOOL).toString('base64')} | base64 -d > $J.receipt-tool.mjs
echo ${Buffer.from(JSON.stringify({ ...receipt, ledger: undefined, receipt_file: J + '.receipt.json', log_file: J + '.out', stderr_file: J + '.err', job_prefix: J, side: 'bb2' })).toString('base64')} | base64 -d > $J.receipt-meta.v2.json
echo ${Buffer.from(inner).toString('base64')} | base64 -d > $J.sh
echo ${Buffer.from(admitted).toString('base64')} | base64 -d > $J.admitted
echo ${Buffer.from(wrapper).toString('base64')} | base64 -d > $J.wrap
touch $J.t0 $J.hb $J.out $J.err
# The wrapper closes this private pipe after publishing its observable PID.
# Slow receipt recovery cannot be mistaken for a vanished job; death before
# acknowledgement closes the pipe without admitting a payload.
ready=$(setsid nohup bash $J.wrap 3>&1 > /dev/null 2>&1 < /dev/null &)
[ "$ready" = BB-WRAPPER-READY ] || {
  echo >&2 ${q('kind=LAUNCH_INFRA step=wrapper-readiness owner=gate-speed recovery=build-slot --remote ' + argv.map(q).join(' '))}
  exit 224
}
echo launched`;
}

// Follow one stream (out|err) from byte offset; reattach after drops. Resolves when the wrapper has exited and the
// stream is fully read (true), or false when it could not reattach within REATTACH_SEC.
async function followStream(J, f, sink, st) {
  let lostAt = 0;
  for (;;) {
    const done = await new Promise((resolve) => {
      // tail exits when the wrapper (writes .rc after the job) is gone and the file is drained.
      const cmd = `w=$(cat ${J}.wpid 2>/dev/null); if [ ! -e ${J}.rc ] && [ -n "$w" ] && kill -0 $w 2>/dev/null; then tail -c +${st[f] + 1} --pid=$w -F ${J}.${f} 2>/dev/null; else tail -c +${st[f] + 1} ${J}.${f}; echo -n >&2 BB-END; fi; t=$?${f === 'err' ? `; printf >&2 '\\nBB_TERMINAL %s %s %s\\n' "$(cat ${J}.rc 2>/dev/null)" "$(base64 -w0 < ${J}.receipt.json 2>/dev/null)" "$(base64 -w0 < ${J}.tdir 2>/dev/null)"` : ''}; exit $t`;
      const p = spawn('ssh', [...SSH, '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=8', HOST, cmd], { stdio: ['ignore', 'pipe', 'pipe'] });
      let err = '';
      p.stdout.on('data', (b) => { st[f] += b.length; sink(b); lostAt = 0; });
      p.stderr.on('data', (b) => { err += b.toString(); });
      p.on('close', (code) => {
        const t = /^BB_TERMINAL (\d*) ([A-Za-z0-9+/=]*) ([A-Za-z0-9+/=]*)$/m.exec(err);
        if (code === 0 && t && t[1] !== '') st.terminal = { rc: Number(t[1]), receipt: Buffer.from(t[2], 'base64'), tdir: Buffer.from(t[3], 'base64').toString().trim() };
        resolve(code === 0 ? 'end' : `ssh rc ${code}: ${err.trim().split('\n').pop()?.slice(0, 120) || ''}`);
      });
      st.procs.add(p); p.on('exit', () => st.procs.delete(p));
    });
    if (done === 'end') return true;
    if (st.stopping) return true;
    if (!lostAt) { lostAt = Date.now(); say(`${f} stream dropped (${done}); reattaching at byte ${st[f]}`); }
    if (Date.now() - lostAt > REATTACH_SEC * 1000) return false;
    await sleep(5000);
  }
}

async function remoteRc(J) {
  const t0 = Date.now();
  for (;;) {
    const r = await sshRun(`cat ${J}.rc 2>/dev/null || { w=$(cat ${J}.wpid 2>/dev/null); [ -n "$w" ] && kill -0 $w 2>/dev/null && echo running || echo gone; }`, { timeout: 45000 });
    const s = r.stdout.toString().trim();
    if (r.status === 0 && /^\d+$/.test(s)) return Number(s);
    if (r.status === 0 && s === 'gone') return null;
    if (Date.now() - t0 > REATTACH_SEC * 1000) return null;
    await sleep(r.status === 0 ? 2000 : 5000);
  }
}

export function copyBackDestination(group, top, rel, env = process.env) {
  if (group.target) return env.CARGO_TARGET_DIR || path.join(top, 'target');
  const destination = env.BUILD_SLOT_REMOTE_FETCH_DIR || path.join(top, rel);
  if (!path.isAbsolute(destination)) throw new Error('copy-back destination must be absolute');
  return destination;
}

async function copyBack(J, top, src, tkey, rel) {
  if (!(process.env.BUILD_SLOT_REMOTE_FETCH || '').trim()) return;
  const m = await sshRun(`cat ${J}.exports/manifest.json`, { timeout: 45000 });
  if (m.status !== 0) throw new Error('missing declared-output manifest');
  for (const group of JSON.parse(m.stdout.toString())) {
    const result = await sshRun(`cat ${q(group.archive)}`, { timeout: 600000 });
    if (result.status !== 0) throw new Error('missing declared-output archive');
    const dest = copyBackDestination(group, top, rel);
    fs.mkdirSync(dest, { recursive: true });
    const extract = spawnSync('tar', ['-xzf', '-', '-C', dest, '--no-same-owner'], { input: result.stdout });
    if (extract.status !== 0) throw new Error(`copy-back extract rc ${extract.status}`);
    say(`copy-back: ${group.count} file(s) from job export into ${dest}`);
  }
  await sshRun(`rm -rf -- ${J}.exports`, { timeout: 45000 });
}

// Returns the job's rc, or 222 when the VM cannot take it (the caller then tries Daytona).
// Host tools the sync and copy-back use (gzip only: the WSL host has no zstd, 2026-10-15). Checked before a job runs, so a
// missing tool sends the job elsewhere instead of losing its outputs after a remote pass.
const HOST_TOOLS = ['ssh', 'rsync', 'tar', 'gzip', 'flock'];
const missingTools = () => HOST_TOOLS.filter((t) => spawnSync('sh', ['-c', `command -v ${t}`], { stdio: 'ignore' }).status !== 0);

// Verdict cache (2026-10-16 compute reduction: 75% of the build box's CPU went to lane and review jobs, many of them the
// same command on the same tree again). A compile-class job that passed is recorded under sha256(repo, the tree's content
// digest (treeIdentity: the exact files the VM would run on), its merge-base with main, the cwd within the checkout, argv, the forwarded env); the
// same key again replays its stdout/stderr and returns 0 without queueing or touching the VM. Only passes are cached (a
// failure always reruns). Not cached: jobs with outputs to copy back (BUILD_SLOT_REMOTE_FETCH), --cold, proof- labels,
// BUILD_SLOT_STRESS=1, BUILD_SLOT_VERDICT_CACHE=0. Entries live 7 days. Main's tree is one content digest, so a main SHA
// is gated once however many lanes ask. A cacheable job whose caller goes away (SIGTERM/SIGHUP: a superseded gate) is
// not killed: it runs to its rc with output to its log only, and a pass is cached for the next caller; a second signal,
// or SIGINT, still kills it.
const VCACHE = process.env.BUILD_SLOT_VERDICT_DIR || '/home/var/.cache/buildbox/verdicts';
const VC_TTL_MS = 7 * 86400e3; const VC_MAX_BYTES = 32 << 20;
const cacheable = (argv) => jobClass(argv) === 'compile' && !COLD && !process.env.BUILD_SLOT_REMOTE_FETCH &&
  !process.env.BUILD_SLOT_GATE_LOG &&
  process.env.BUILD_SLOT_VERDICT_CACHE !== '0' && process.env.BUILD_SLOT_STRESS !== '1' &&
  !/^proof-/.test(process.env.BUILD_SLOT_LABEL || '') && !/^proof-/.test(path.basename(process.cwd()));
function verdictKey(repo, id, rel, argv, base = '') {  // base: the merge-base with main (affected-tests style scripts diff against it)
  const env = passEnv().split('\n').filter((l) => l && !l.startsWith('export BB_CRIT_REASON=')).sort().join('\n');
  return crypto.createHash('sha256').update(JSON.stringify(['v1', repo, id.content, base, rel, argv, env])).digest('hex');
}
const vfile = (k, ext) => `${VCACHE}/${k.slice(0, 2)}/${k}.${ext}`;
// Hold before admission: identical waiters consume neither compile nor source-copy slots.
function lockVerdict(k) {
  const file = vfile(k, 'lock'); fs.mkdirSync(path.dirname(file), { recursive: true });
  return new Promise((resolve, reject) => {
    const child = spawn('flock', ['-w', '1800', '-E', '75', file, 'sh', '-c', 'echo held; exec cat >/dev/null'],
      { cwd: '/', stdio: ['pipe', 'pipe', 'pipe'] });
    let output = '', error = '';
    child.on('error', reject);
    child.stderr.on('data', b => { error += b; });
    child.on('exit', code => reject(new Error(`verdict lock failed (rc=${code}): ${error.trim()}`)));
    child.stdout.on('data', b => {
      output += b;
      if (output.includes('held\n')) { child.stdout.destroy(); child.unref(); child.stdin.unref?.(); resolve(child); }
    });
  });
}

function verdictGet(k) {
  try { const m = JSON.parse(fs.readFileSync(vfile(k, 'json'), 'utf8'));
    return m.rc === 0 && Date.now() - Date.parse(m.at) < VC_TTL_MS ? m : null; } catch { return null; }
}
function verdictPut(k, meta, outBuf, errBuf) {
  try {
    if (outBuf.length + errBuf.length > VC_MAX_BYTES) return;
    fs.mkdirSync(`${VCACHE}/${k.slice(0, 2)}`, { recursive: true }); const t = `${VCACHE}/${k.slice(0, 2)}/.${k}.${process.pid}`;
    fs.writeFileSync(`${t}.out`, outBuf); fs.renameSync(`${t}.out`, vfile(k, 'out'));
    fs.writeFileSync(`${t}.err`, errBuf); fs.renameSync(`${t}.err`, vfile(k, 'err'));
    fs.writeFileSync(`${t}.json`, JSON.stringify(meta)); fs.renameSync(`${t}.json`, vfile(k, 'json'));  // last: an entry exists only when complete
    say(`verdict cache: stored this pass (${k.slice(0, 12)})`);
  } catch (e) { say(`verdict cache: store failed (${e.message.slice(0, 120)})`); }
}
function verdictReplay(k, m, id, cwd, argv, t0) {
  let o, e; try { o = fs.readFileSync(vfile(k, 'out')); e = fs.readFileSync(vfile(k, 'err')); } catch { return null; }
  say(`BUILD_SLOT_TREE=${id.commit} ${id.tree} ${id.content} (client ${cwd})`);
  say(`verdict cache: this tree and command passed at ${m.at} (${m.label || '?'}, ${m.wall}s); replaying its output, not rerunning (BUILD_SLOT_VERDICT_CACHE=0 forces a run)`);
  process.stdout.write(o); process.stderr.write(e);

  return 0;
}
// cargo check made redundant by clippy (2026-10-16 compute reduction): clippy runs the same check with lints on top. In a
// bash/sh -c job script, "cargo check A &&" / "cargo check A;" / "cargo check A<newline>" is dropped when "cargo clippy A"
// (the same arguments up to clippy's own " -- ", ignoring -q/--quiet) follows later in the same script.
const CK_WORDS = String.raw`((?: +[^\s;&|<>()$'"\\` + '`' + String.raw`]+)*)`;
function dropCoveredCheck(argv) {
  const a0 = path.basename(argv[0] || ''); const ci = argv.findIndex((x, i) => i > 0 && /^-[a-z]*c$/.test(x));
  if (!['bash', 'sh'].includes(a0) || ci < 1 || ci + 1 >= argv.length || process.env.BUILD_SLOT_KEEP_CHECK === '1') return argv;
  const norm = (x) => x.trim().split(/ +/).filter((w) => w && w !== '--quiet' && w !== '-q').join(' ');
  const dropped = [];
  const sc = argv[ci + 1].replace(new RegExp(String.raw`(^ *|[\n;&|(] *)cargo (\+\S+ )?check` + CK_WORDS + String.raw` *(&&|;|\n)`, 'g'), (all, pre, tc, args, sep, off, whole) => {
    const later = whole.slice(off + all.length); const want = norm(args);
    const cl = new RegExp(String.raw`(^ *|[\n;&|(] *)cargo ` + (tc || '').replace(/[+.]/g, '\\$&') + 'clippy' + CK_WORDS, 'g');
    if (![...later.matchAll(cl)].some((x) => norm(x[2].split(' -- ')[0]) === want)) return all;
    dropped.push(`cargo ${tc || ''}check${args}`); return pre;
  });
  if (!dropped.length) return argv;
  say(`skipping ${dropped.join('; ')}: cargo clippy with the same arguments follows in this job (BUILD_SLOT_KEEP_CHECK=1 keeps it)`);
  const r = argv.slice(); r[ci + 1] = sc; return r;
}
// nextest per-test ceiling (2026-10-16 compute reduction): neither cloud nor sdk sets a timeout in nextest's default
// profile, so a hung test held its job until the job ceiling. Every `cargo nextest run` on the box gets a tool config
// (lowest precedence: a repo's own .config/nextest.toml settings win) whose default profile terminates a test after
// 10 min (60 s x 10). Written into the job's private /tmp by the job prelude. Not with BUILD_SLOT_STRESS=1.
const NEXTEST_TOOL = '/tmp/bb-nextest.toml';  // also written literally in the cargo job env block
// Per-binary test ceiling (2026-10-18, crit-bslot: three plain `cargo test -p cloudctl` binaries deadlocked in futex waits and
// held merge-tier compile slots 40-75 min; nextest's slow-timeout never applies to plain cargo test). A cargo target runner
// runs every test binary (target/**/deps/*: cargo test, nextest, bench) under timeout, default 900 s per binary invocation
// (BUILD_SLOT_TEST_BIN_MAX_SEC can only lower it); cargo run binaries (target/<profile>/<bin>) are untouched. Not with
// BUILD_SLOT_STRESS=1, nor when the caller set its own runner.
function testRunner() {
  if (process.env.BUILD_SLOT_STRESS === '1' || process.env.CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER) return '';
  const want = Number(process.env.BUILD_SLOT_TEST_BIN_MAX_SEC || 900); const sec = Math.max(30, Math.min(900, Number.isFinite(want) ? want : 900));
  const sh = `#!/bin/sh
case "$1" in */deps/*) ;; *) exec "$@";; esac
timeout --foreground -k 15 ${sec} "$@"; rc=$?
[ $rc -eq 124 ] || [ $rc -eq 137 ] && echo "[buildbox] test binary $(basename "$1") still running after ${sec}s: killed (rc $rc). Per-binary ceiling for cargo test; BUILD_SLOT_TEST_BIN_MAX_SEC lowers it, BUILD_SLOT_STRESS=1 lifts it" >&2
exit $rc
`;
  return `echo ${Buffer.from(sh).toString('base64')} | base64 -d > /tmp/bb-test-runner && chmod +x /tmp/bb-test-runner && export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=/tmp/bb-test-runner
`;
}
function nextestTimeout(argv) {
  if (process.env.BUILD_SLOT_STRESS === '1') return argv;
  const flag = `--tool-config-file bb:${NEXTEST_TOOL}`;
  if (path.basename(argv[0] || '') === 'cargo') {
    const i = argv.indexOf('nextest'); if (i < 1 || argv[i + 1] !== 'run' || argv.includes('--tool-config-file')) return argv;
    if (argv.slice(1, i).some((x) => !x.startsWith('+'))) return argv;
    return [...argv.slice(0, i + 2), '--tool-config-file', `bb:${NEXTEST_TOOL}`, ...argv.slice(i + 2)];
  }
  const ci = argv.findIndex((x, i) => i > 0 && /^-[a-z]*c$/.test(x));
  if (!['bash', 'sh'].includes(path.basename(argv[0] || '')) || ci < 1 || ci + 1 >= argv.length || argv[ci + 1].includes('--tool-config-file')) return argv;
  const sc = argv[ci + 1].replace(/(\bcargo (?:\+\S+ )?nextest run)(?=[ \n;&|)]|$)/g, `$1 ${flag}`);
  if (sc === argv[ci + 1]) return argv;
  const r = argv.slice(); r[ci + 1] = sc; return r;
}
// Host-runnable binaries (2026-10-17, cl-glibc): bb1 has glibc 2.39 and the WSL host 2.35, so a binary linked natively on
// bb1 and copied back failed "GLIBC_2.3x not found" (lanes detoured through ubuntu:22.04 containers). A remote cargo build
// whose outputs come back (BUILD_SLOT_REMOTE_FETCH) runs as `cargo zigbuild --target x86_64-unknown-linux-gnu.<host glibc>`
// (cargo-zigbuild + zig, pinned in bb-rebuild TOOLS): zig links against that glibc version's symbols, the job keeps its
// warm shared target slot and sccache, and fetch paths /work/target/<profile>/... are read from the triple dir, so callers
// still fetch /work/target/debug/<bin>. The host version comes from getconf; BUILD_SLOT_GLIBC=2.NN overrides it and
// BUILD_SLOT_GLIBC=native links natively (a binary meant for bb1-like hosts). An explicit --target is left alone.
const GLIBC_TRIPLE = 'x86_64-unknown-linux-gnu';
let glibcPin = null;  // { version, dirs: profile dirs under the triple dir } for this job
let buildJob = false;  // cargo build/zigbuild job: named target files come back even when cargo found them fresh
function hostGlibc() {
  const o = spawnSync('getconf', ['GNU_LIBC_VERSION'], { encoding: 'utf8' });
  return (/glibc (\d+\.\d+)/.exec(o.stdout || '') || [])[1] || '';
}
function pinGlibc(argv) {
  glibcPin = null; buildJob = false;
  if (path.basename(argv[0] || '') !== 'cargo') return argv;
  const i = (argv[1] || '').startsWith('+') ? 2 : 1;
  if (!['build', 'b', 'zigbuild'].includes(argv[i])) return argv;
  buildJob = true;
  if (argv[i] === 'zigbuild' || !process.env.BUILD_SLOT_REMOTE_FETCH) return argv;
  const cut = argv.indexOf('--'); const opts = cut < 0 ? argv : argv.slice(0, cut);
  if (opts.some((a) => a === '--target' || a.startsWith('--target='))) return argv;
  const want = process.env.BUILD_SLOT_GLIBC || hostGlibc();
  if (!want || want === 'native') return argv;
  if (!/^2\.\d+$/.test(want)) { say(`BUILD_SLOT_GLIBC=${want}: want 2.NN or native; linking natively (bb1 glibc)`); return argv; }
  const dirs = new Set(['debug', 'release']);
  opts.forEach((a, k) => { if (a === '--profile') dirs.add(opts[k + 1]); else if (a.startsWith('--profile=')) dirs.add(a.slice(10)); });
  glibcPin = { version: want, dirs };
  say(`fetched build: cargo zigbuild --target ${GLIBC_TRIPLE}.${want} so the binaries run on this host (glibc ${want}); BUILD_SLOT_GLIBC=native links against bb1's glibc`);
  return [...argv.slice(0, i), 'zigbuild', '--target', `${GLIBC_TRIPLE}.${want}`, ...argv.slice(i + 1)];
}
// Cold box notice (2026-10-18, crit-bslot): buildbox-ctl heal writes COLD_MARK when it auto-starts a deallocated VM;
// deallocation wipes the local NVMe /mnt/bb (targets, sccache), so for 90 min every job says why its build is slow.
const COLD_MARK = '/home/var/.ssh/buildbox/cold-start';
function coldNotice() {
  try { const st = fs.statSync(COLD_MARK); const min = Math.round((Date.now() - st.mtimeMs) / 60000); if (min > 90) return;
    say(`COLD BOX: bb1 was auto-started ${min} min ago after a deallocation (${fs.readFileSync(COLD_MARK, 'utf8').trim().slice(0, 160)}); its local /mnt/bb was wiped; bb-warm restores sccache and one hot target per flavour from the persistent disk (saved every 30 min), the rest rebuilds: expect slower first builds`); } catch {}
}
export function beginReceipt(argv) {
  // arm installs this job's watcher. Historical recovery is an explicit owner
  // operation, never a host-wide prerequisite of unrelated job admission.
  const stamp = Date.now() + '-' + process.pid + '-' + crypto.randomUUID().slice(0, 8);
  const logFile = `${LOGDIR}/${stamp}.log`, metaFile = `${LOGDIR}/${stamp}.receipt-meta.v2.json`;
  fs.closeSync(fs.openSync(logFile, 'a'));
  const prs = (process.env.BUILD_SLOT_PR || '').split(/[+,\s]+/).filter(x => /^\d+$/.test(x)).map(Number);
  const meta = { schema_version: 1, repo: 'unknown', lane: process.env.BUILD_SLOT_LABEL || path.basename(process.cwd()),
    worktree: process.cwd(), admission_backend: 'bb2', phase: process.env.BUILD_SLOT_PHASE || 'command', gate_log: process.env.BUILD_SLOT_GATE_LOG || null, local_preparation_seconds: Number(process.env.BUILD_SLOT_GATE_PREPARATION_SECONDS || 0), pr_number: prs.length === 1 ? prs[0] : null, pr_numbers: prs,
    source: null, argv: [...argv], log_id: stamp, log_file: logFile, receipt_file: logFile + '.receipt.json',
    admission_start: new Date().toISOString(), ledger: LOGDIR + '/jobs.jsonl', side: 'client' };
  arm(metaFile, meta);
  const ctx = { meta, metaFile, logFile, remote: null };
  try {
    ctx.co = checkout(process.cwd()); meta.repo = ctx.co.repo;
    atomic(metaFile, { ...JSON.parse(fs.readFileSync(metaFile, 'utf8')), ...meta });
  } catch {}
  return ctx;
}
export function endReceipt(ctx, rc) {
  return finish(ctx.metaFile, rc, ctx.termination || ctx.remote?.termination || (cause || rc === 224 || rc === 222 ? 'infra' : rc >= 128 ? 'interrupted' : 'command'),
    { reason: ctx.failure?.cause || cause || ctx.remote?.reason || null, detail: ctx.failure?.detail || causeDetail || ctx.remote?.detail || null, remote_receipt: ctx.remote, cache: ctx.remote?.cache, admission: ctx.remote?.admission, disk_free: ctx.remote?.disk_free, home_removed: ctx.remote?.home_removed, home_leaks: ctx.remote?.home_leaks });
}
export async function run(argv, sharedReceipt) {
  if (sharedReceipt) return runTracked(argv, sharedReceipt);
  const ctx = beginReceipt(argv);
  let rc = 224;
  try { rc = await runTracked(argv, ctx); return rc; }
  finally {
    endReceipt(ctx, rc);
  }
}
async function runTracked(argv, ctx) {
  try { return await runTrackedOnce(argv, ctx); } finally { ctx.early?.cancel(); }
}
async function runTrackedOnce(argv, ctx) {
  const t0 = Date.now(); used(); cause = ''; causeDetail = ''; coldNotice();
  const miss = missingTools();
  if (miss.length) { say(`host lacks ${miss.join(', ')} (install with apt); not using the build box`); return unavailable('host-tools-missing', miss.join(' ')); }
  mark('enter'); const cwd = process.cwd(); let co;
  try { co = ctx.co || checkout(cwd); } catch (e) { say(`no checkout root for ${cwd}: ${e.message.slice(0, 120)}`); return unavailable('no-checkout', cwd); }
  const top = co.top; const rel = path.relative(top, cwd) || '.'; mark('checkout');
  { const live = BOXES.filter((h) => !fs.existsSync(disabledMark(h)));
    if (live.length === 1 && process.env.BUILD_SLOT_BUILDBOX_RSYNC !== '1') { setHost(live[0]); co.early = ctx.early = preconnect(); } }
  // Before anything identifies or replays: an attested Windows snapshot, or none.
  try { co.snapshot = await consumeClientSnapshot(co.top); } catch (e) { process.stderr.write(`${e.message}\n`); return unavailable('tree-identity', e.message); }
  mark('snapshot');
  ctx.meta.repo = co.repo;
  argv = pinGlibc(nextestTimeout(dropCoveredCheck(argv)));
  const comparisonBase = co.snapshot ? co.snapshot.base : mergeBase(co); let verdictLock = null; mark('base');
  const prerequisites = admitInputs(argv, cwd, comparisonBase);
  if (!prerequisites) return unavailable('input-closure', 'required input admission refused; see typed diagnostic above');
  ctx.meta.input_closure = prerequisites;
  // Pin the dispatched source and comparison base to the same identity as its verdict lock.
  let id; try { id = co.snapshot ? co.snapshot.id : identityFor(co); } catch (e) { return unavailable('sync-failed', `tree identity: ${e.message}`); }
  mark('identity');
  ctx.meta.argv = [...argv];
  ctx.meta.source = { commit: id.commit, git_tree: id.uploadedTree || id.tree, base_tree: id.tree,
    dirty_overlay_sha256: id.overlay || crypto.createHash('sha256').update(id.patch || id.content).digest('hex'), content_sha256: id.content };
  atomic(ctx.metaFile, { ...JSON.parse(fs.readFileSync(ctx.metaFile, 'utf8')), ...ctx.meta });
  try {
    if (cacheable(argv)) {
      const k = verdictKey(co.repo, id, rel, argv, comparisonBase); let m = verdictGet(k);
      if (m) { const r = verdictReplay(k, m, id, cwd, argv, t0); if (r !== null) { ctx.meta.execution_kind = 'existing-verdict-replay'; atomic(ctx.metaFile, { ...JSON.parse(fs.readFileSync(ctx.metaFile, 'utf8')), ...ctx.meta }); return r; } }
      verdictLock = await lockVerdict(k);
      m = verdictGet(k);
      if (m) { const r = verdictReplay(k, m, id, cwd, argv, t0); if (r !== null) { ctx.meta.execution_kind = 'existing-verdict-replay'; atomic(ctx.metaFile, { ...JSON.parse(fs.readFileSync(ctx.metaFile, 'utf8')), ...ctx.meta }); return r; } }
    }
  mark('verdict'); { const why = await pickHost(); if (why) { say(`no build box reachable (${why})`); return unavailable('vm-unreachable', why); } }
  // Target keys are per repo (sdk: /mnt/bb/targets/sdk/<key>); cloud's are unchanged.
  const base = keyOf(top); let tkey = REPOS[co.repo].tkey + (process.env.CARGO_TARGET_DIR ? keyOf(path.resolve(process.env.CARGO_TARGET_DIR)) : base);
  // build-slot --remote --cold: a fresh target dir for this job only (removed after copy-back) and no sccache.
  if (COLD) tkey = `cold/${Date.now()}-${process.pid}`;
  // Concurrent jobs from one checkout (a node gate's control/local/ci/tsc steps, shards) each get their own remote copy
  // <base>.<n> for the whole job, so one job's rsync --delete or npm ci never rewrites a tree another job is running in.
  mark('host'); const n = await claimCopy(base); mark('copy'); if (n === null) { say(`all ${COPIES} remote copies of ${base} busy`); return unavailable('copies-busy', `${COPIES} copies of ${base}`); }
  try { configureRemoteTier(cwd); return await runIn(argv, t0, cwd, co, rel, `${base}.${n}`, tkey, id, comparisonBase, ctx); } finally { releaseCopy(base, n); used(); mark('done'); ctx.meta.phases = { ...PHASES }; say(`BUILD_SLOT_PHASES ${JSON.stringify(PHASES)}`); }
  } finally { verdictLock?.stdin.end(); }
}

// Client command classification is a dispatch hint; bb2 owns admission policy.
const LIGHT_CARGO = new Set(['fmt', 'metadata', 'locate-project', 'tree', 'pkgid', 'version', '--version', '-V', 'search', 'verify-project']);
function admitInputs(argv, root, base) {
  const policy = fileURLToPath(new URL('./input_closure.py', import.meta.url));
  const result = spawnSync('python3', ['-B', policy, 'validate', '--stage', 'client', '--root', root,
    '--base', base || 'none', '--', ...argv], { encoding: 'utf8', env: process.env });
  if (result.status !== 0) {
    if (result.stderr) process.stderr.write(result.stderr);
    if (!/^kind=INPUT_CLOSURE /m.test(result.stderr || '')) process.stderr.write(`kind=INPUT_CLOSURE step=policy-exec input=declaration owner=gate-speed recovery=${q('python3 -B '+q(policy)+' --help')} detail=${q(result.error?.message || 'policy did not complete with a typed result')}\n`);
    return null;
  }
  const { proof, declaration } = JSON.parse(result.stdout);
  process.env.BUILD_SLOT_INPUTS_JSON = JSON.stringify(declaration);
  process.env.BUILD_SLOT_INPUT_BASE = base || 'none';
  delete process.env.BUILD_SLOT_INPUTS_FILE;
  // bb2 references have not been observed on the client. Do not reuse a
  // verdict before their actual host admission has occurred.
  if (declaration.inputs.some(item => item.where === 'bb2')) process.env.BUILD_SLOT_VERDICT_CACHE = '0';
  return proof;
}
const COMPILE_CARGO = /\bcargo\s+(\+\S+\s+)?(build|b|check|c|clippy|test|t|nextest|doc|d|run|r|bench|install|llvm-cov|miri|udeps|hack)\b|\bbuild-slot\b|\bnode-gate|\baffected-tests|\bbun run build|\bnpm run build|\bwasm-pack\b|\bjust\b|\bmake\b/;
// Light class (2026-10-18, crit-bslot: a format check or git commit queued 20-25 min behind cargo in the compile FIFO):
// a command, or a bash/sh -c script, every one of whose simple commands is light (format checks, git, node static checks,
// test -f, npm ci, shell builtins and text tools) skips admission entirely ('light' is logged, never queued). Anything
// unknown is not light. Opaque job scripts (bash x.sh, python3 x.py, base64-wrapped inline code) are compile when their
// text runs cargo/rustc/nextest: they used to take 'other' slots and loaded the box past the compile cap.
const LIGHT_WORDS = new Set(['rustfmt', 'true', 'false', 'echo', 'printf', 'cat', 'ls', 'git', 'pwd', 'env', 'id', 'uname', 'sha256sum',
  'sha1sum', 'md5sum', 'stat', 'test', '[', '[[', ']', ']]', 'set', 'export', 'unset', 'cd', 'mkdir', 'rm', 'cp', 'mv', 'touch', 'grep', 'rg',
  'sed', 'awk', 'head', 'tail', 'wc', 'sort', 'uniq', 'diff', 'cmp', 'tr', 'cut', 'date', 'basename', 'dirname', 'realpath', 'readlink',
  'jq', 'tee', 'command', 'which', 'type', 'exit', 'shopt', 'trap', 'local', 'declare', 'if', 'then', 'else', 'elif', 'fi', 'case', 'esac',
  'shellcheck', 'shfmt', 'gh', 'du', 'df', 'file', 'chmod', 'ln', 'base64', 'xxd', 'od', 'hexdump', 'nproc', 'free', 'uptime', 'hostname', 'whoami']);
const LIGHT_NODE = /(^|\/)(static|javascript-check|[\w.-]*(format|fmt|lint)[\w.-]*)\.(mjs|cjs|js)$/;
function lightSegment(seg) {
  let w = seg.trim().replace(/^[!({)\s]+/, '').split(/\s+/).filter(Boolean);
  for (;;) {  // strip prefixes: VAR=val, time, nice -n N, timeout [-k N] DUR, env
    if (!w.length) return true;
    if (/^[A-Za-z_]\w*=/.test(w[0])) { w = w.slice(1); continue; }
    if (w[0] === 'time' || w[0] === 'env' && w.length > 1) { w = w.slice(1); continue; }
    if (w[0] === 'nice') { w = w.slice(w[1] === '-n' ? 3 : 1); continue; }
    if (w[0] === 'timeout') { let i = 1; while (w[i] && w[i].startsWith('-')) i += /^(-k|-s|--kill-after|--signal)$/.test(w[i]) ? 2 : 1; w = w.slice(i + 1); continue; }
    break;
  }
  const a0 = path.basename(w[0]).replace(/[)}]+$/, ''); const rest = w.slice(1).filter((x) => !x.startsWith('-'));
  if (!a0 || /^[)}\]]+$/.test(w[0])) return true;
  if (LIGHT_WORDS.has(a0)) return true;
  if (a0 === 'cargo') return LIGHT_CARGO.has(w.slice(1).find((x) => !x.startsWith('+') && !/^--?(q|quiet|locked|offline|frozen)$/.test(x)) || '');
  if (a0 === 'tofu' || a0 === 'terraform') return ['fmt', 'version', '-version'].includes(rest[0] || w[1]);
  if (a0 === 'node') return ['--check', '-c', '--version', '-v'].includes(w[1]) || LIGHT_NODE.test(rest[0] || '');
  if (a0 === 'npm') return ['ci', 'install', 'i', 'ls', 'view', 'pkg', 'config'].includes(rest[0] || '') && !/\bbuild\b/.test(seg) || w.includes('--version');
  if (a0 === 'npx') return /^(prettier|@biomejs\/biome|biome)$/.test(rest[0] || '');
  return false;
}
function isLight(argv) {
  const a0 = path.basename(argv[0] || '');
  if (['bash', 'sh'].includes(a0)) {
    const ci = argv.findIndex((x, i) => i > 0 && /^-[a-z]*c$/.test(x));
    if (ci < 1 || ci + 1 >= argv.length) return false;  // bash file.sh: opaque
    const sc = argv[ci + 1];
    if (sc.length > 4000 || /<<|\bfunction\b|\bwhile\b|\bfor\b|\buntil\b|\beval\b|\bsource\b|(^|[\s;&|])\.\s/.test(sc)) return false;
    return sc.replace(/\$\(|`/g, '\n').split(/\n|;|&&|\|\||\|/).every(lightSegment);
  }
  return lightSegment(argv.map((x) => /\s/.test(x) ? 'X' : x).join(' '));
}
const OPAQUE_CARGO = /\bcargo\b["'`,\s)\]\[]+(?:\+[^\s"',]+["',\s]+)?(?:["']?-[^\s"',]*["',\s]+)*["']?(build|b|check|c|clippy|test|t|nextest|run|r|bench|llvm-cov|miri|doc|zigbuild)\b|\brustc\b|\bnextest\b|\bbuild-slot\b|\$\{?CARGO\b/;
function opaqueText(argv) {
  const parts = [argv.join(' ')];
  for (const x of argv) {
    for (const m of x.matchAll(/[A-Za-z0-9+/]{40,}={0,2}/g)) { try { parts.push(Buffer.from(m[0], 'base64').toString('utf8')); } catch {} }
    if (/\.(sh|bash|py)$/.test(x) && !x.includes('\n')) { try { const f = path.resolve(process.cwd(), x); if (fs.statSync(f).size < 1 << 20) parts.push(fs.readFileSync(f, 'utf8')); } catch {} }
  }
  return parts.join('\n');
}
function jobClass(argv) {
  const a0 = path.basename(argv[0] || '');
  if (a0 === 'cargo') { const sub = argv.slice(1).find((x) => !x.startsWith('+')) || ''; return LIGHT_CARGO.has(sub) ? '' : 'compile'; }
  if (['rustfmt', 'true', 'echo', 'cat', 'ls', 'git', 'pwd', 'env', 'id', 'uname', 'sha256sum', 'stat', 'test'].includes(a0)) return '';
  if (argv.length === 2 && /^(-v|--version)$/.test(argv[1])) return '';
  if (COMPILE_CARGO.test(argv.join(' '))) return 'compile';
  if (isLight(argv)) return 'light';
  return OPAQUE_CARGO.test(opaqueText(argv)) ? 'compile' : 'other';
}
const pidAlive = (pid) => { try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; } };
// Job ceiling (2026-10-16 compute reduction; two lane jobs sat deadlocked for 4.5 h and 7.8 h): a VM-side watchdog kills a
// job past its wall-clock ceiling (rc 124). Lane and proof jobs: 30 min. Merge gates (tier 0/1), qualification
// (BUILD_SLOT_TIER=qual or a qual- label) and explicit stress runs (BUILD_SLOT_STRESS=1): 3 h. BUILD_SLOT_JOB_MAX_SEC can
// only lower a ceiling.
let jobTier = 2;
function jobMaxSec() {
  const long = jobTier <= 1 || process.env.BUILD_SLOT_STRESS === '1' || /^qual(ification)?$/.test(process.env.BUILD_SLOT_TIER || '') ||
    /^qual-/.test(process.env.BUILD_SLOT_LABEL || '');
  const cap = long ? 10800 : 1800; const want = Number(process.env.BUILD_SLOT_JOB_MAX_SEC || cap);
  return Math.max(60, Math.min(cap, Number.isFinite(want) ? want : cap));
}
function tenantOf(label, cwd, repo, argv = []) {
  const tenant = process.env.BUILD_SLOT_TENANT;
  if (tenant === 'graphcoder' || tenant === 'cloud') return tenant;
  throw new Error('kind=CPU_LEASE step=tenant-label recovery='+q('BUILD_SLOT_TENANT=graphcoder /home/var/bin/build-slot --remote '+argv.map(q).join(' '))+' detail="set graphcoder for SDK or cloud for Cloud; explicit tenant required"');
}
function configureRemoteTier(cwd) {
  const label = process.env.BUILD_SLOT_LABEL || path.basename(cwd);
  const proof = /^proof-/.test(label) || /^proof-/.test(path.basename(cwd));
  const critical = !proof && /^crit-/.test(label) && !!process.env.BUILD_SLOT_CRIT_REASON;
  const merge = !proof && (process.env.BUILD_SLOT_TIER === 'merge' || process.env.BUILD_SLOT_PRIORITY === '1' || /merge/i.test(cwd));
  jobTier = critical ? 0 : merge ? 1 : proof ? 3 : 2;
}

const COPIES = Number(process.env.BUILD_SLOT_BUILDBOX_COPIES || 32); const CLAIMS = '/home/var/.cache/buildbox/claims';
// Copy claims (2026-10-16 claimfix; a crit shard died on "ENOENT stat claims/<base>.16"): the old mkdir claim stat'ed a
// claim its owner was releasing at that moment, and two jobs could both rm+mkdir ("reclaim") one dead claim, or reclaim
// a claim whose owner had not written its pid yet. A claim is now a kernel lock: a `flock -n` child holds
// <claims>/<base>.<n>.lock for the job's life and exits when this process does (its stdin pipe closes, even on SIGKILL),
// so a dead job's claim frees itself and nothing ever reaps or steals a claim. Lock files are never deleted (deleting
// a flock file races its next locker). The file records the owner (pid, boot id, time, label) for listings only.
// Legacy claim dirs <base>.<n> (wrappers loaded before this version) still count as busy while their pid lives.
const BOOT_ID = (() => { try { return fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim(); } catch { return ''; } })();
const heldCopies = new Map();
function lockCopy(file) {
  return new Promise((resolve) => {
    let c; try { c = spawn('flock', ['-n', '-E', '75', file, 'sh', '-c', 'echo held; exec cat >/dev/null'], { cwd: '/', stdio: ['pipe', 'pipe', 'ignore'] }); } catch { resolve(null); return; }
    let out = '';
    c.on('error', () => resolve(null)); c.on('exit', () => resolve(null));  // 75: another job holds it
    c.stdout.on('data', (b) => { out += b; if (out.includes('held')) { c.stdout.destroy(); c.unref(); c.stdin.unref?.(); resolve(c); } });
  });
}
function legacyClaimLive(d) {
  let pid = 0; try { pid = Number(fs.readFileSync(`${d}/pid`, 'utf8').trim()) || 0; } catch {}
  if (pid) return pidAlive(pid);
  try { return Date.now() - fs.statSync(d).mtimeMs < 30e3; } catch { return false; }  // mkdir'd, pid not yet written
}
async function claimCopy(base) {
  fs.mkdirSync(CLAIMS, { recursive: true });
  for (let n = 0; n < COPIES; n++) {
    const d = `${CLAIMS}/${base}.${n}`;
    const c = await lockCopy(`${d}.lock`); if (!c) continue;
    if (fs.existsSync(d)) {
      if (legacyClaimLive(d)) { c.stdin.end(); continue; }
      try { fs.rmSync(d, { recursive: true, force: true }); } catch {}  // a dead legacy claim; we hold the lock
    }
    try { fs.writeFileSync(`${d}.lock`, `${process.pid} ${BOOT_ID} ${new Date().toISOString()} ${process.env.BUILD_SLOT_LABEL || ''}\n`); } catch {}
    heldCopies.set(`${base}.${n}`, c); return n;
  }
  return null;
}
function releaseCopy(base, n) { const c = heldCopies.get(`${base}.${n}`); heldCopies.delete(`${base}.${n}`); try { c?.stdin.end(); } catch {} }

// The client's merge-base with origin/main (sent to the job prelude); checkout() already made Windows gitdirs readable.
function mergeBase(co) {
  let b = ''; try { const r = co.git(['merge-base', 'HEAD', 'refs/remotes/origin/main'], { encoding: 'utf8', timeout: 30000 }); if (r.status === 0) b = String(r.stdout).trim(); } catch {}
  return /^[0-9a-f]{40}$/.test(b) ? b : '';
}
async function maintainSourceCopies(repo, notes, run = sshRun) {
  if (repo !== 'sdk') return;
  const recovery = q('ssh bb2 /home/var/bin/prune-wt-sdk.sh');
  try {
    const prune = await run('/home/var/bin/prune-wt-sdk.sh', { timeout: 120000 });
    const diagnostic = String(prune.stderr ?? '').trim();
    const warnings = diagnostic.split(/\r?\n/).filter(line => /^BB_WT_PRUNE_FAILED(?:\s|$)/u.test(line));
    if (prune.status === 0 && warnings.length === 0) return;
    const detail = (prune.status === 0 ? warnings.join('\n') : diagnostic)
      || String(prune.error?.message || prune.signal || 'transport returned no diagnostic; maintenance result unobserved');
    const reason = `BB_WT_PRUNE_FAILED rc=${prune.status} kind=WORKTREE_MAINTENANCE step=source-retirement owner=gate-speed recovery=${recovery}: ${detail}`;
    notes.push(reason); say(reason);
  } catch (error) {
    const reason = `BB_WT_PRUNE_FAILED kind=WORKTREE_MAINTENANCE step=source-retirement owner=gate-speed recovery=${recovery}: ${error.message || String(error) || 'maintenance exception without diagnostic'}`;
    notes.push(reason); say(reason);
  }
  // Maintenance cannot deny another worktree. Its own preparation and the
  // job's real filesystem free-space admission remain mandatory below.
}
async function runIn(argv, t0, cwd, co, rel, key, tkey, id, base, ctx) {
  const J = `/mnt/bb/jobs/${Date.now()}-${process.pid}-${crypto.randomUUID().slice(0, 8)}`; const top = co.top; const R = REPOS[co.repo];
  // A client-side flock vanishes on a WSL restart while the detached bb2 job
  // remains alive. Every upload therefore owns a unique remote source copy;
  // recovering a local claim can never reset/clean a running job's checkout.
  key = sourceCopyKey(key, J);
  const logFile = ctx.logFile;
  Object.assign(ctx.meta, { key, tkey, job_id: J.slice(13), gate_log: process.env.BUILD_SLOT_GATE_LOG || null });
  atomic(ctx.metaFile, { ...JSON.parse(fs.readFileSync(ctx.metaFile, 'utf8')), ...ctx.meta });
  const receipt = { ...ctx.meta, client_receipt: ctx.meta.receipt_file, job_id: J.slice(13) };
  // Git sync + launch in one ssh; rsync + launch when the checkout cannot use it (cloud only).
  let src = `${R.wt}/${key}`; const notes = [];
  // id and base are the immutable inputs used by run() to own this verdict key.
  say(`BUILD_SLOT_TREE=${id.commit} ${id.tree} ${id.content} (client ${top})`);
  const maint = {};
  const g = process.env.BUILD_SLOT_BUILDBOX_RSYNC === '1' && R.rsync ? 'fallback' : await gitPrepLaunch(co, src, launchScript(src, tkey, rel, argv, J, R.path, co.repo, base, id, jobMaxSec(), process.env.BUILD_SLOT_IDLE_OK === '1', receipt), notes, J, id, maint);
  mark('launched');
  await maintainSourceCopies(co.repo, notes, async () => maint.result ?? { status: 255, stderr: Buffer.alloc(0) }); mark('maintain');
  if (g === 'unreachable') { heal(notes.join('; ') || 'ssh failed'); return unavailable('vm-unreachable', notes.join('; ') || 'ssh failed'); }
  if (g === 'fallback' && !R.rsync) return unavailable('sync-failed', `${co.repo} git sync: ${notes.join('; ')}`);
  if (g === 'fallback') {
    const why = await reachable();
    if (why) { heal(why); return unavailable('vm-unreachable', why); }
    src = `/mnt/bb/src/${key}`;
    try { notes.push(sync(co, key, id.files)); } catch (e) { heal(e.message); return unavailable('sync-failed', e.message); }
    const l = await sshRun('bash -s', { input: launchScript(src, tkey, rel, argv, J, R.path, co.repo, base, id, jobMaxSec(), process.env.BUILD_SLOT_IDLE_OK === '1', receipt), timeout: 90000 });
    if (l.status !== 0 || !/launched/.test(l.stdout.toString())) { heal(`launch failed: ssh rc ${l.status} ${l.stderr.slice(-120)}`); return unavailable('launch-failed', `ssh rc ${l.status} ${l.stderr.slice(-120)}`); }
  }
  say(`${HOST}: ${notes.join('; ')}; src ${src.slice(8)} target ${tkey}; job ${J.slice(13)}; ${argv.join(' ').slice(0, 160)}`);
  const log = fs.createWriteStream(logFile);
  const st = { out: 0, err: 0, procs: new Set(), stopping: false, detached: false };
  const vkey = cacheable(argv) ? verdictKey(co.repo, id, rel, argv, base) : null; const vo = [], ve = []; let vbytes = 0;
  const keep = (a, b) => { if (!vkey || vbytes > VC_MAX_BYTES) return; vbytes += b.length; a.push(Buffer.from(b)); };
  process.stdout.on('error', () => {}); process.stderr.on('error', () => {});  // a caller gone away (EPIPE) never kills the job
  const hb = setInterval(() => { sshRun(`touch ${J}.hb`, { timeout: 45000 }); used(); }, 60000);
  for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP']) process.on(sig, async () => {
    if (vkey && sig !== 'SIGINT' && !st.detached) {
      st.detached = true; try { log.write(`[build-slot buildbox] ${ts()} ${sig}: caller gone; job ${J.slice(13)} runs on for the verdict cache\n`); } catch {}
      return;
    }
    st.stopping = true; clearInterval(hb);
    await sshRun(`touch ${J}.cancelled; p=$(cat ${J}.pid 2>/dev/null) && kill -TERM -- -$p; sleep 3; kill -KILL -- -$p 2>/dev/null; true`, { timeout: 20000 });
    for (const p of st.procs) p.kill('SIGTERM');
    finish(ctx.metaFile, 128 + (sig === 'SIGINT' ? 2 : sig === 'SIGTERM' ? 15 : 1), 'cancelled', { reason: sig });
    process.exit(128 + (sig === 'SIGINT' ? 2 : sig === 'SIGTERM' ? 15 : 1));
  });
  const [okOut, okErr] = await Promise.all([
    followStream(J, 'out', (b) => { if (!st.detached) process.stdout.write(b); log.write(b); keep(vo, b); }, st),
    followStream(J, 'err', (b) => {
      if (!st.detached) process.stderr.write(b); log.write(b); keep(ve, b);
      st.receiptLines = (st.receiptLines || '') + b.toString();
      const lines = st.receiptLines.split('\n'); st.receiptLines = lines.pop().slice(-65536);
      for (const line of lines) if (line.startsWith('BUILD_SLOT_EXEC=')) {
        mark('exec-line');
        try { atomic(ctx.metaFile + '.exec', JSON.parse(line.slice(16))); } catch {}
      }
    }, st)]);
  mark('streams'); let rc = !(okOut && okErr) ? null : st.terminal ? st.terminal.rc : await remoteRc(J); mark('rc');
  clearInterval(hb); log.end();
  if (rc === null) {
    const recovery = `ssh bb2 ${q('cat '+J+'.receipt.json')}`;
    say(`kind=JOB_CUSTODY step=follow-existing-job owner=gate-speed job=${J.slice(13)} recovery=${q(recovery)} detail='transport ended without an authoritative terminal receipt; preserve the existing job'`);
    // Transport absence grants no process identity or termination authority.
    // Its existing receipt watcher/watchdog retains custody; recovery consumes
    // that same job rather than signalling a potentially reused numeric PID.
    return unavailable('job-lost', `terminal receipt unobserved; recovery=${recovery}`);
  }
  // The wrapper writes the receipt before .rc, so a follower that saw .rc also carried the final receipt.
  const terminal = st.terminal?.receipt.length ? { status: 0, stdout: st.terminal.receipt } : await sshRun(`cat ${J}.receipt.json`, { timeout: 45000 });
  if (terminal.status === 0) {
    try {
      ctx.remote = JSON.parse(terminal.stdout.toString());
      atomic(ctx.metaFile + '.exec', { exec_start: ctx.remote.exec_start, toolchain: ctx.remote.toolchain });
    } catch (e) { say(`receipt read failed: ${e.message}`); }
  }
  say(`rc=${rc} in ${((Date.now() - t0) / 1000).toFixed(1)}s; log ${logFile}`);
  if (rc === 0 && process.env.BUILD_SLOT_REMOTE_FETCH) jobTdir = st.terminal?.tdir || String((await sshRun(`cat ${J}.tdir 2>/dev/null`, { timeout: 45000 })).stdout || '').trim();
  if (rc === 0) {
    try { await copyBack(J, top, src, tkey, rel); }
    catch (e) { say(`copy-back FAILED (${e.message.slice(0, 200)}); the job passed remotely but its declared outputs are not local`); rc = 1; ctx.termination = 'infra'; }
  }
  if (rc === 0 && vkey && vbytes <= VC_MAX_BYTES) verdictPut(vkey, { at: new Date().toISOString(), rc, wall: Math.round((Date.now() - t0) / 1000), label: process.env.BUILD_SLOT_LABEL || path.basename(cwd), cwd, tree: `${id.commit} ${id.tree} ${id.content}`, cmd: argv.join(' ').slice(0, 300) }, Buffer.concat(vo), Buffer.concat(ve));
  // Keep bb2 logs, metadata and terminal receipt together for accounting/recovery.
  if (COLD) sshRun(`rm -rf /mnt/bb/targets/${tkey}`, { timeout: 120000 });

  return rc;
}

function sourceCopyKey(key, jobPrefix) {
  return `${key}-${path.basename(jobPrefix)}`;
}

// bs.ps1's Windows adapter entry: `node buildbox.mjs snapshot --out F` / `snapshot-attest --snapshot F` (cwd: the checkout).
if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url)) && /^snapshot(-attest)?$/.test(process.argv[2] || '')) {
  const [verb, flag, file, then] = process.argv.slice(2);
  if (!file || flag !== (verb === 'snapshot' ? '--out' : '--snapshot') || (then !== undefined && (verb !== 'snapshot' || then !== '--attest'))) {
    process.stderr.write('usage: buildbox.mjs snapshot --out F [--attest] | snapshot-attest --snapshot F\n'); process.exit(2);
  }
  const write = (out, value) => { fs.writeFileSync(`${out}.${process.pid}`, JSON.stringify(value)); fs.renameSync(`${out}.${process.pid}`, out); };
  let value; try { value = verb === 'snapshot' ? clientSnapshot() : clientAttestation(); }
  catch (e) { value = { kind: SNAPSHOT_KIND, version: 1, error: e.message.slice(0, 300) }; process.stderr.write(`${snapshotRefusal(`snapshot failed: ${value.error}`).message}\n`); }
  write(verb === 'snapshot' ? file : `${file}.attest`, value);
  if (!value.error && then === '--attest') { value = clientAttestation(); write(`${file}.attest`, value); }
  process.exit(value.error ? 224 : 0);
}
