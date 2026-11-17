import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import test from 'node:test';

test('lock checking rejects stale local identities and broken edges without rewriting input', () => {
  const root = mkdtempSync(join(tmpdir(), 'sdk-lock-'));
  try {
    mkdirSync(join(root, 'zeta'));
    writeFileSync(join(root, 'Cargo.toml'), '[workspace]\nmembers = ["zeta"]\n');
    writeFileSync(join(root, 'zeta/Cargo.toml'), '[package]\nname = "zeta"\nversion = "0.1.0"\n');
    const lock = 'version = 4\n\n[[package]]\nname = "zeta"\nversion = "0.1.0"\n';
    const file = join(root, 'Cargo.lock');
    const check = () => spawnSync(process.execPath, [resolve('scripts/check-cargo-lock.mjs')], {cwd: root, encoding: 'utf8'});
    writeFileSync(file, lock);
    assert.equal(check().status, 0);
    writeFileSync(file, lock.replace('0.1.0', '0.0.0'));
    const stale = check();
    assert.equal(stale.status, 1);
    assert.match(stale.stderr, /missing local package zeta@0.1.0/);
    assert.equal(readFileSync(file, 'utf8'), lock.replace('0.1.0', '0.0.0'));
    writeFileSync(file, lock + 'dependencies = ["missing"]\n');
    const broken = check();
    assert.equal(broken.status, 1);
    assert.match(broken.stderr, /missing dependency missing/);
  } finally {rmSync(root, {recursive: true, force: true});}
});