import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, mkdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';

// Cargo discovers target configuration from the working directory. The
// independent fixture copies the workspace profiles without building its crates.
const root = process.cwd();
const fixture = mkdtempSync(join(tmpdir(), 'sdk-build-profile-'));
const target = resolve(process.env.CARGO_TARGET_DIR ?? 'target', 'profile-contract');
function run(command, args) {
  const result = spawnSync(command, args, {cwd: root, encoding: 'utf8', maxBuffer: 1 << 23, env: {...process.env, CARGO_TARGET_DIR: target}});
  if (result.error) throw result.error;
  assert.equal(result.status, 0, `${command} ${args.join(' ')}\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}
function build(args, level, compilerArgs = []) {
  const output = run('cargo', [...args, '--locked', '--manifest-path', join(fixture, 'Cargo.toml'), '--message-format=json', ...compilerArgs]);
  const artifacts = output.split('\n').filter(line => line.startsWith('{')).map(line => JSON.parse(line)).filter(item => item.reason === 'compiler-artifact' && item.package_id.includes('build-profile-contract'));
  assert.ok(artifacts.length > 0, 'Cargo reported no fixture artifacts');
  for (const artifact of artifacts) assert.equal(artifact.profile.opt_level, level, 'effective optimization profile');
  return artifacts;
}
try {
  mkdirSync(join(fixture, 'src'));
  const workspace = readFileSync(join(root, 'Cargo.toml'), 'utf8');
  const profiles = workspace.slice(workspace.indexOf('[profile.dev]'));
  assert.ok(profiles.startsWith('[profile.dev]'), 'workspace has no dev profile');
  writeFileSync(join(fixture, 'Cargo.toml'), '[package]\nname="build-profile-contract"\nversion="0.1.0"\nedition="2024"\n[workspace]\n' + profiles);
  writeFileSync(join(fixture, 'src/main.rs'), 'fn main() { println!("profile contract"); }\n#[cfg(test)] mod tests { #[test] fn panic_is_caught() { assert!(std::panic::catch_unwind(|| panic!("expected")).is_err()); } }\n');
  run('cargo', ['generate-lockfile', '--offline', '--manifest-path', join(fixture, 'Cargo.toml')]);
  const binaries = build(['build'], '0');
  build(['test'], '0');
  const host = run('rustc', ['-vV']);
  if (host.includes('host: x86_64-unknown-linux-gnu')) {
    for (const artifact of binaries.filter(item => item.executable)) {
      assert.match(run('readelf', ['--string-dump=.comment', artifact.executable]), /mold/, 'native Linux executable must be linked by mold');
    }
  }
  const releaseDeps = join(target, 'release/deps');
  mkdirSync(releaseDeps, {recursive: true});
  for (const name of readdirSync(releaseDeps).filter(name => name.startsWith('build_profile_contract-') && name.endsWith('.ll'))) rmSync(join(releaseDeps, name));
  build(['rustc', '--release'], '3', ['--', '--emit=llvm-ir']);
  assert.ok(readdirSync(releaseDeps).some(name => name.startsWith('build_profile_contract-') && name.endsWith('.ll')), 'release must produce LLVM IR');
  console.log('Dev and test use opt-level 0; panic recovery, native mold linking and release LLVM passed');
} finally {
  rmSync(fixture, {recursive: true, force: true});
}
