import assert from 'node:assert/strict';
import {execFileSync, spawnSync} from 'node:child_process';
import {copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import test from 'node:test';

test('Cargo canonical output passes and a resolution-valid reordered lock fails', () => {
  const root = mkdtempSync(join(tmpdir(), 'sdk-cargo-lock-'));
  const checker = resolve('scripts/check-cargo-lock.mjs');
  try {
    for (const directory of ['src', 'zeta/src']) mkdirSync(join(root, directory), {recursive: true});
    copyFileSync('rust-toolchain.toml', join(root, 'rust-toolchain.toml'));
    writeFileSync(join(root, 'Cargo.toml'), `[package]
name = "alpha"
version = "0.0.0"
edition = "2024"
[dependencies]
zeta = { path = "zeta" }
[workspace]
members = ["zeta"]
resolver = "3"
`);
    writeFileSync(join(root, 'zeta/Cargo.toml'), '[package]\nname = "zeta"\nversion = "0.0.0"\nedition = "2024"\n');
    for (const source of ['src/lib.rs', 'zeta/src/lib.rs']) writeFileSync(join(root, source), '');
    // The fixture lockfile is captured from real Cargo output, never authored.
    execFileSync('cargo', ['metadata', '--offline', '--format-version', '1'], {cwd: root, stdio: 'ignore'});
    const lock = join(root, 'Cargo.lock');
    const canonical = readFileSync(lock, 'utf8');
    const check = () => spawnSync(process.execPath, [checker], {cwd: root, encoding: 'utf8'});
    const positive = check();
    assert.equal(positive.status, 0, positive.stderr);
    assert.equal(readFileSync(lock, 'utf8'), canonical);

    const [header, ...packages] = canonical.split('[[package]]\n');
    assert.equal(packages.length, 2);
    const reordered = header + packages.reverse().map(block => '[[package]]\n' + block).join('');
    writeFileSync(lock, reordered);
    execFileSync('cargo', ['metadata', '--locked', '--offline', '--format-version', '1'], {cwd: root, stdio: 'ignore'});
    assert.equal(readFileSync(lock, 'utf8'), reordered, '--locked accepts the planted ordering defect');
    const negative = check();
    assert.equal(negative.status, 1, negative.stderr);
    assert.match(negative.stderr, /step=cargo-lock-canonical:.*canonical output.*recovery command: cargo metadata/);
    assert.equal(readFileSync(lock, 'utf8'), canonical, 'Cargo restores its own exact original bytes');
  } finally {
    rmSync(root, {recursive: true, force: true});
  }
});
