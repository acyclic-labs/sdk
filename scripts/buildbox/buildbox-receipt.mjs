import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export function atomic(file, value) {
  const tmp = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`;
  const fd = fs.openSync(tmp, 'wx', 0o600);
  try { fs.writeFileSync(fd, JSON.stringify(value) + '\n'); fs.fsyncSync(fd); }
  finally { fs.closeSync(fd); }
  fs.renameSync(tmp, file);
  const parent = fs.openSync(path.dirname(file), 'r');
  try { fs.fsyncSync(parent); } finally { fs.closeSync(parent); }
}
// A PID is meaningful only in the namespace in which it was recorded.
export function pidNamespace() {
  try { return fs.readlinkSync('/proc/self/ns/pid'); } catch { return null; }
}
export function canObserveOwner(owner) {
  const namespace = pidNamespace();
  return namespace !== null && owner?.pid_namespace === namespace;
}
function namespaceRecord() {
  const ns = pidNamespace();
  if (!ns) return null;
  const record = read(`/mnt/bb/jobs/${ns.replaceAll(/[^0-9]/g, '')}.pid-namespace.json`);
  return record?.namespace === ns && record.init_identity === processIdentity(1) ? record : null;
}
function endedNamespace(m) {
  // An outer wrapper waits for namespace init. Only a registered namespace and
  // its completed owner can prove that invisible child PIDs have exited.
  const scope = m.namespace_scope;
  if (!scope || scope.namespace !== m.owner?.pid_namespace || !scope.parent_receipt_meta) return false;
  const parent = read(scope.parent_receipt_meta), registered = read(scope.registration_file);
  const terminal = parent?.receipt_file && read(parent.receipt_file);
  return Boolean(parent?.receipt_file && terminal && registered?.epoch && registered.epoch === scope.epoch &&
    registered.parent_receipt_meta === scope.parent_receipt_meta &&
    terminal.receipt_file === parent.receipt_file && terminal.namespace_custody === 'pidns-v1');
}
export function processIdentity(pid) {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8').split(') ').pop().split(' ');
    if (stat[0] === 'Z') return null;
    return fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim() + ':' + stat[19];
  } catch { return null; }
}
const read = f => { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return null; } };
export function arm(file, meta, pid = process.pid) {
  const scope = namespaceRecord();
  atomic(file, { exec_start: null, toolchain: null, ...meta,
    namespace_custody: 'pidns-v1', namespace_scope: scope,
    owner: { pid: Number(pid), identity: processIdentity(pid), pid_namespace: pidNamespace() } });
  if (scope) fs.appendFileSync(scope.registration_file + '.children', JSON.stringify(file) + '\n', {mode:0o600});
  const p = spawn(process.execPath, [new URL(import.meta.url).pathname, 'watch', file], {
    detached: true, stdio: 'ignore'
  });
  p.unref();
}
export function execution(metaFile) {
  const meta = read(metaFile);
  const a = meta.argv;
  const override = a.find(x => x.startsWith('+'));
  const r = spawnSync('rustc', [...(override ? [override] : []), '-vV'], { encoding: 'utf8', timeout: 15000 });
  const vv = r.status === 0 ? r.stdout : null;
  const targets = [];
  for (let i = 0; i < a.length; i++) {
    if (a[i] === '--target' && a[i + 1]) targets.push(a[++i]);
    else if (a[i].startsWith('--target=')) targets.push(a[i].slice(9));
  }
  if (!targets.length) targets.push(process.env.CARGO_BUILD_TARGET || /^host: (.+)$/m.exec(vv || '')?.[1] || 'unknown');
  const value = {
    exec_start: new Date().toISOString(),
    toolchain: { rustc_vv_sha256: vv ? crypto.createHash('sha256').update(vv).digest('hex') : null,
      rustc_vv: vv, target: [...new Set(targets)], probe_error: vv ? null : String(r.stderr || r.error || 'rustc unavailable') }
  };
  atomic(metaFile + '.exec', value);
  return value;
}
function kernelWriter(file, operation, value) {
  // Kernel custody ends on exit, including SIGKILL. Legacy mkdir locks are
  // deliberately not consulted or stolen: their PID coordinates are unknown.
  fs.closeSync(fs.openSync(file + '.writer-lock', 'a', 0o600));
  const writer = spawnSync('flock', ['--exclusive', '--no-fork', file + '.writer-lock',
    process.execPath, new URL(import.meta.url).pathname, operation], {
    input: JSON.stringify(value), encoding: 'utf8', maxBuffer: 1 << 28
  });
  if (writer.stderr) process.stderr.write(writer.stderr);
  if (writer.status !== 0) throw new Error(`receipt writer failed: ${writer.status ?? writer.signal ?? writer.error}`);
  return JSON.parse(writer.stdout);
}
export function finish(metaFile, rc, category, extra = {}) {
  const m = read(metaFile);
  if (!m) throw new Error(`missing receipt metadata: ${metaFile}`);
  if (fs.existsSync(m.receipt_file)) return read(m.receipt_file);
  return kernelWriter(m.receipt_file, 'locked-finish', {metaFile, rc, category, extra});
}
function finishLocked(metaFile, rc, category, extra = {}) {
  const m = read(metaFile);
  if (!m) throw new Error(`missing receipt metadata: ${metaFile}`);
  const file = m.receipt_file;
  if (fs.existsSync(file)) return read(file);
  const foreignEnded = !canObserveOwner(m.owner) && category === 'interrupted' && endedNamespace(m);
  if (!canObserveOwner(m.owner) && !foreignEnded) return null;
  {
    if (fs.existsSync(file)) return read(file);
    const runtimeFile = metaFile + '.runtime';
    let runtime, cleanupAllowed = true;
    if (!foreignEnded && m.side === 'bb2' && category !== 'command' && read(metaFile + '.job')) {
      const stopped = spawnSync('python3', ['/home/var/bin/bb-job.py', 'stop-job', metaFile + '.job'], { encoding: 'utf8', timeout: 15000 });
      if (stopped.status !== 0) {
        cleanupAllowed = false;
        extra = {...extra, namespace_custody:'unproven', cleanup_deferred:String(stopped.stderr || stopped.error)};
      }
    }
    runtime = read(runtimeFile);
    if (cleanupAllowed && runtime && m.side === 'bb2') {
      const clean = spawnSync('python3', ['/home/var/bin/bb-job.py', 'cleanup', runtimeFile], { encoding: 'utf8', timeout: 30000 });
      runtime = read(runtimeFile);
      if (clean.status !== 0) {
        runtime.cleanup_error = String(clean.stderr || clean.error);
        rc = 224; category = 'infra';
      }
    }
    if (cleanupAllowed && m.side === 'bb2' && m.job_prefix) {
      const clean = spawnSync('python3', ['/home/var/bin/bb-job.py', 'cleanup-outer', m.job_prefix], { encoding: 'utf8', timeout: 30000 });
      runtime ||= {};
      if (clean.status === 0) Object.assign(runtime, JSON.parse(clean.stdout));
      else {
        runtime.outer_cleanup_error = String(clean.stderr || clean.error);
        rc = 224; category = 'infra';
      }
    }
    const exec = read(metaFile + '.exec');
    const out = { exec_start: null, toolchain: null, ...m, ...exec, end: new Date().toISOString(), exit_status: Number(rc), termination: category,
      ...runtime, ...extra };
    // Retain the original ledger's basic fields for existing cost readers.
    out.at = out.end; out.rc = out.exit_status; out.cwd = out.worktree;
    out.wall = (Date.parse(out.end) - Date.parse(out.admission_start)) / 1000;
    out.cmd = out.argv.map(x => JSON.stringify(x)).join(' ');
    if (out.source) out.tree = [out.source.commit, out.source.base_tree, out.source.content_sha256 || out.source.git_tree].join(' ');
    delete out.owner; delete out.ledger;
    atomic(file, out);
    if (m.ledger) ledgerRecord(m.ledger, out);
    // Constructor-owned indexes visit only this job's descendants. Finish
    // their interrupted receipts now, without rescanning unrelated jobs.
    let registrations = '';
    try { registrations = fs.readFileSync(metaFile + '.pid-namespaces','utf8'); } catch {}
    for (const entry of registrations.split('\n').filter(Boolean)) {
      let children = '';
      try { children = fs.readFileSync(JSON.parse(entry) + '.children','utf8'); } catch {}
      for (const child of children.split('\n').filter(Boolean)) {
        const childFile = JSON.parse(child), metadata = read(childFile);
        if (metadata && endedNamespace(metadata)) finish(childFile,137,'interrupted',{reason:'enclosing PID namespace ended'});
      }
    }
    return out;
  }
}
function ledgerRecord(ledger, out) {
  return kernelWriter(ledger, 'locked-ledger', {ledger, out});
}
function ledgerRecordLocked(ledger, out) {
    let lines = [], previous = '';
    try { previous = fs.readFileSync(ledger, 'utf8'); lines = previous.split('\n'); } catch {}
    if (!lines.some(s => { try { return JSON.parse(s).receipt_file === out.receipt_file; } catch { return false; } }))
      {
        const fd = fs.openSync(ledger, 'a');
        try { fs.writeFileSync(fd, (previous && !previous.endsWith('\n') ? '\n' : '') + JSON.stringify(out) + '\n'); fs.fsyncSync(fd); }
        finally { fs.closeSync(fd); }
      }
  return true;
}
export async function watch(metaFile, interval = 1000) {
  const m = read(metaFile);
  if (!canObserveOwner(m?.owner)) return;
  for (;;) {
    if (fs.existsSync(m.receipt_file)) {
      if (m.ledger) ledgerRecord(m.ledger, read(m.receipt_file));
      return;
    }
    if (processIdentity(m.owner.pid) !== m.owner.identity) {
      const job = read(metaFile + '.job');
      if (canObserveOwner(job) && processIdentity(job.pid) === job.identity) {
        try { process.kill(-job.pid, 'SIGTERM'); } catch {}
        await new Promise(r => setTimeout(r, 1000));
        if (processIdentity(job.pid) === job.identity) try { process.kill(-job.pid, 'SIGKILL'); } catch {}
      }
      const result = finish(metaFile, 137, 'interrupted', { reason: 'receipt owner disappeared' });
      if (result && m.job_prefix) {
        const tmp = m.job_prefix + '.rc.receipt-tmp';
        fs.writeFileSync(tmp, '137\n'); fs.renameSync(tmp, m.job_prefix + '.rc');
      }
      if (result) return;
    }
    await new Promise(r => setTimeout(r, interval));
  }
}
export function recover(dir) {
  const seen = new Map();
  for (const name of fs.readdirSync(dir)) {
    if (!name.endsWith('.receipt-meta.v2.json') && !name.endsWith('.receipt-meta.json')) continue;
    const file = path.join(dir, name), m = read(file);
    const terminal = m?.receipt_file && read(m.receipt_file);
    if (terminal && m.ledger && canObserveOwner(m.owner)) {
      if (!seen.has(m.ledger)) {
        let records = [];
        try { records = fs.readFileSync(m.ledger, 'utf8').split('\n'); } catch {}
        seen.set(m.ledger, new Set(records.map(s => { try { return JSON.parse(s).receipt_file; } catch { return null; } })));
      }
      if (!seen.get(m.ledger).has(terminal.receipt_file)) {
        ledgerRecord(m.ledger, terminal); seen.get(m.ledger).add(terminal.receipt_file);
      }
    }
    if (m?.owner && !fs.existsSync(m.receipt_file) &&
        (canObserveOwner(m.owner) && processIdentity(m.owner.pid) !== m.owner.identity || endedNamespace(m))) {
      const job = read(file + '.job');
      if (canObserveOwner(job) && processIdentity(job.pid) === job.identity) {
        const p = spawn(process.execPath, [new URL(import.meta.url).pathname, 'watch', file], { detached: true, stdio: 'ignore' });
        p.unref(); continue;
      }
      const result = finish(file, 137, 'interrupted', { reason: 'receipt owner absent during recovery' });
      if (result && m.job_prefix) {
        const tmp = m.job_prefix + '.rc.receipt-tmp';
        fs.writeFileSync(tmp, '137\n'); fs.renameSync(tmp, m.job_prefix + '.rc');
      }
    }
  }
}
// Native bb2 entry point, including scripts which invoke build-slot recursively.
// Children retain a parent_log_id so cost readers never sum parent and child time.
export async function runBb(script, args) {
  const dir = '/mnt/bb/jobs'; fs.mkdirSync(dir, { recursive: true }); recover(dir);
  const stamp = Date.now() + '-' + process.pid + '-' + crypto.randomUUID().slice(0, 8);
  const prefix = `${dir}/native-${stamp}`, file = prefix + '.receipt-meta.v2.json';
  const parentFile = process.env.BUILD_SLOT_RECEIPT_META || namespaceRecord()?.parent_receipt_meta;
  const parent = parentFile && read(parentFile);
  const prs = (process.env.BUILD_SLOT_PR || '').split(/[+,\s]+/).filter(x => /^\d+$/.test(x)).map(Number);
  const argv = process.env.BUILD_SLOT_RECEIPT_ARGV ? JSON.parse(process.env.BUILD_SLOT_RECEIPT_ARGV) : [...args]; while (/^--(remote|local|cold)$/.test(argv[0] || '')) argv.shift();
  let source = parent?.source || null, repo = parent?.repo || 'unknown';
  const git = (a, env = process.env) => {
    const r = spawnSync('git', a, { env, encoding: 'utf8', maxBuffer: 1 << 28 });
    if (r.status !== 0) throw new Error(String(r.stderr)); return r.stdout.trim();
  };
  const meta = { schema_version: 1, admission_backend: 'bb2', repo, lane: process.env.BUILD_SLOT_LABEL || parent?.lane || path.basename(process.cwd()),
    worktree: process.cwd(), phase: process.env.BUILD_SLOT_PHASE || parent?.phase || 'native', gate_log: process.env.BUILD_SLOT_GATE_LOG || parent?.gate_log || null, local_preparation_seconds: Number(process.env.BUILD_SLOT_GATE_PREPARATION_SECONDS || 0), pr_number: parent?.pr_number || (prs.length === 1 ? prs[0] : null), pr_numbers: parent?.pr_numbers || prs,
    source, argv, log_id: stamp, log_file: prefix + '.log', receipt_file: prefix + '.receipt.json',
    admission_start: process.env.BUILD_SLOT_ADMISSION_START || new Date().toISOString(), side: 'bb2', parent_log_id: parent?.log_id || null,
    parent_receipt_meta: parent ? parentFile : null, accounting_scope: parent ? 'child' : 'job' };
  arm(file, meta);
  let child, termination, rc = 224;
  const signal = sig => {
    termination = sig === 'SIGINT' ? 'cancelled' : 'interrupted';
    if (child) try { process.kill(-child.pid, sig); } catch {}
  };
  const handlers = new Map(['SIGINT', 'SIGTERM', 'SIGHUP'].map(sig => [sig, () => signal(sig)]));
  for (const [sig, handler] of handlers) process.on(sig, handler);
  try {
    if (!source) {
      try {
        const top = git(['rev-parse', '--show-toplevel']);
        const index = prefix + '.index', env = { ...process.env, GIT_INDEX_FILE: index };
        try {
          git(['-C', top, 'read-tree', 'HEAD'], env); git(['-C', top, 'add', '-A'], env);
          source = { commit: git(['rev-parse', 'HEAD']), base_tree: git(['rev-parse', 'HEAD^{tree}']),
            git_tree: git(['-C', top, 'write-tree'], env),
            dirty_overlay_sha256: crypto.createHash('sha256').update(git(['-C', top, 'diff', '--cached', '--binary', 'HEAD'], env)).digest('hex') };
          repo = /acyclic-labs[/:]([^/]+?)(?:\.git)?$/.exec(git(['config', '--get', 'remote.origin.url']))?.[1] || repo;
        } finally { fs.rmSync(index, { force: true }); fs.rmSync(index + '.lock', { force: true }); }
      } catch { /* no uploaded tree exists for commands outside a checkout */ }
    }
    atomic(file, { ...read(file), source, repo });
    if (process.env.BUILD_SLOT_DEFER_EXEC !== '1') execution(file);
    const log = fs.createWriteStream(meta.log_file);
    child = spawn(script, args, { env: { ...process.env, BUILD_SLOT_RECEIPT_WRAPPED: '1', BUILD_SLOT_RECEIPT_META: file },
      detached: true, stdio: ['inherit', 'pipe', 'pipe'] });
    child.on('error', () => {});
    await new Promise((resolve, reject) => { child.once('spawn', resolve); child.once('error', reject); });
    atomic(file + '.job', { pid: child.pid, identity: processIdentity(child.pid), pid_namespace: pidNamespace() });
    child.stdout.on('data', b => { process.stdout.write(b); log.write(b); });
    child.stderr.on('data', b => { process.stderr.write(b); log.write(b); });
    const end = await new Promise(resolve => child.once('close', (code, sig) => resolve({ code, sig })));
    rc = end.code ?? (128 + ({ SIGKILL: 9, SIGTERM: 15, SIGINT: 2, SIGHUP: 1 }[end.sig] || 1));
    await new Promise(resolve => log.end(resolve));
    termination ||= end.sig ? 'interrupted' : rc === 224 || rc === 222 ? 'infra' : 'command';
    return rc;
  } finally {
    for (const [sig, handler] of handlers) process.removeListener(sig, handler);
    const terminal = finish(file, rc, termination || 'infra');
    if (!terminal) { process.stderr.write('[buildbox] receipt INFRA: terminal ownership is unproven\n'); return 224; }
    if (terminal && terminal.exit_status !== rc) return terminal.exit_status;
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [op, file, rc, category] = process.argv.slice(2);
  if (op === 'run-bb') process.exitCode = await runBb(file, process.argv.slice(4));
  else if (op === 'arm') arm(file, read(file), rc);
  else if (op === 'job') atomic(file + '.job', { pid: Number(rc), identity: processIdentity(rc), pid_namespace: pidNamespace() });
  else if (op === 'exec') process.stderr.write('BUILD_SLOT_EXEC=' + JSON.stringify(execution(file)) + '\n');
  else if (op === 'finish') {
    const terminal = finish(file, rc, category);
    if (!terminal) { process.stderr.write('[buildbox] receipt INFRA: terminal ownership is unproven\n'); process.exitCode = 224; }
    if (terminal && terminal.exit_status !== Number(rc)) process.exitCode = terminal.exit_status;
  }
  else if (op === 'watch') await watch(file);
  else if (op === 'recover') recover(file);
  else if (op === 'locked-finish') {
    const {metaFile, rc, category, extra} = JSON.parse(fs.readFileSync(0, 'utf8'));
    process.stdout.write(JSON.stringify(finishLocked(metaFile, rc, category, extra)));
  }
  else if (op === 'locked-ledger') {
    const {ledger, out} = JSON.parse(fs.readFileSync(0, 'utf8'));
    process.stdout.write(JSON.stringify(ledgerRecordLocked(ledger, out)));
  }
  else throw new Error(`unknown receipt operation: ${op}`);
}
