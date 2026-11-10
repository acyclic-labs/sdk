import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmodSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { assemble } from './assemble-graphcoder-release.mjs';
import { verifyNativeImage } from './verify-release-binary.mjs';

// Real-binary scenario, explicitly selected by the release jobs on all three OSes.
const binary = process.env.GRAPHCODER_TEST_BINARY;
assert.ok(binary && isAbsolute(binary), 'Set GRAPHCODER_TEST_BINARY to the source-qualified GraphCoder executable');
const bytes = readFileSync(binary);
const native = verifyNativeImage(bytes);
const target = native.startsWith('linux-')
  ? verifyNativeImage(bytes, `${native}-${bytes.includes(Buffer.from('ld-linux')) ? 'gnu' : 'musl'}`) : native;
const version = JSON.parse(readFileSync(new URL('../plugin/package.json', import.meta.url), 'utf8')).version;
const root = mkdtempSync(join(tmpdir(), 'graphcoder-install-'));
const digest = contents => createHash('sha256').update(contents).digest('hex');
const run = (command, args, env = process.env) => {
  const result = spawnSync(command, args, { env, encoding: 'utf8', windowsHide: true });
  assert.ifError(result.error);
  assert.equal(result.signal, null, result.stderr);
  return result;
};
try {
  const assets = join(root, 'release assets'), prefix = join(root, 'installed bin');
  // Missing coverage and wrong architectures fail before publishing any assets.
  assert.throws(() => assemble({ binaries: [`${target}=${binary}`], output: assets, universal: true }), /Missing/);
  const other = target.startsWith('win32-') ? 'darwin-arm64' : 'win32-x64';
  assert.throws(() => assemble({ binaries: [`${other}=${binary}`], output: assets }));
  assert.deepEqual(readdirSync(root), []);
  const names = assemble({ binaries: [`${target}=${binary}`], output: assets });
  const asset = names.find(name => name.startsWith('graphcoder-'));
  // Exercise the existing inventory producer and verifier, not a test-only manifest writer.
  const evidence = fileURLToPath(new URL('./release-evidence.mjs', import.meta.url));
  const sums = join(assets, 'SHA256SUMS');
  const produced = run(process.execPath, [evidence, 'checksums', sums, ...names.map(name => join(assets, name))]);
  assert.equal(produced.status, 0, produced.stderr);
  const verified = run(process.execPath, [evidence, 'verify', sums]);
  assert.equal(verified.status, 0, verified.stderr);
  const manifest = readFileSync(sums, 'utf8');
  const windows = process.platform === 'win32';
  const destination = join(prefix, windows ? 'graphcoder.exe' : 'graphcoder');
  const env = { ...process.env, GRAPHCODER_INSTALL_DIR: prefix, GRAPHCODER_ASSET_DIR: assets };
  const install = () => run(windows ? 'pwsh' : 'sh', windows
    ? ['-NoProfile', '-File', join(assets, 'install-graphcoder.ps1')]
    : [join(assets, 'install-graphcoder.sh')], env);
  const accept = () => {
    const result = install();
    assert.equal(result.status, 0, result.stderr);
    assert.equal(digest(readFileSync(destination)), digest(bytes));
    const launched = run(destination, ['--version']);
    assert.equal(launched.status, 0, launched.stderr);
    assert.equal(launched.stdout.trim(), `graphcoder ${version}`);
    assert.deepEqual(readdirSync(prefix), [windows ? 'graphcoder.exe' : 'graphcoder']);
  };
  accept();
  // Replacement of an existing binary exercises the native overwrite operation.
  writeFileSync(destination, 'previous executable');
  accept();
  const refuse = () => {
    const result = install();
    assert.notEqual(result.status, 0, result.stdout);
    assert.equal(digest(readFileSync(destination)), digest(bytes), 'failed install changed the installed executable');
    assert.deepEqual(readdirSync(prefix), [windows ? 'graphcoder.exe' : 'graphcoder']);
  };
  writeFileSync(join(assets, asset), bytes.subarray(0, 128));
  refuse(); // corrupt download
  writeFileSync(join(assets, asset), bytes);
  rmSync(join(assets, asset));
  refuse(); // incomplete download/local directory
  writeFileSync(join(assets, asset), bytes);
  const entry = `${digest(bytes)}  ${asset}\n`;
  writeFileSync(sums, manifest + entry);
  refuse(); // ambiguous checksum
  writeFileSync(sums, manifest.replace(entry, ''));
  refuse(); // missing checksum
  writeFileSync(sums, `${'0'.repeat(64)}  ${asset}\n`);
  refuse(); // mismatched checksum
  // A checksum is necessary but insufficient: never publish a non-runnable image.
  writeFileSync(join(assets, asset), bytes.subarray(0, 128));
  chmodSync(join(assets, asset), 0o755);
  writeFileSync(sums, `${digest(bytes.subarray(0, 128))}  ${asset}\n`);
  refuse();
  writeFileSync(join(assets, asset), bytes);
  writeFileSync(sums, manifest);
  const script = join(assets, windows ? 'install-graphcoder.ps1' : 'install-graphcoder.sh');
  writeFileSync(script, readFileSync(script, 'utf8').replaceAll(version, '0.0.0-negative-control'));
  refuse(); // runnable bytes with the wrong release identity
  console.log(`INSTALL_RELEASE_ASSET_RESULT PASS platform=${process.platform} target=${target} controls=11`);
} finally {
  // mkdtemp owns this exact child of the OS temporary root.
  rmSync(root, { recursive: true, force: true });
}
