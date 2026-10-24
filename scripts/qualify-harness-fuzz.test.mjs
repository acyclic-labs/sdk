import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const runner = fileURLToPath(new URL('./qualify-harness-fuzz.sh', import.meta.url));

function qualify(t, discovery) {
  const directory = mkdtempSync(join(tmpdir(), 'harness-fuzz-discovery-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const bin = join(directory, 'bin');
  mkdirSync(bin);
  writeFileSync(join(directory, 'Cargo.lock'), '# fixture lockfile\n');
  const calls = join(directory, 'cargo-calls');
  writeFileSync(join(bin, 'cargo'), '#!/bin/bash\nprintf "%s\\n" "$*" >> "$CARGO_CALLS_LOG"\n', { mode: 0o755 });
  writeFileSync(join(bin, 'python3'), `#!/bin/bash\n${discovery}\n`, { mode: 0o755 });
  const result = spawnSync('bash', [runner], {
    cwd: directory,
    env: { ...process.env, PATH: `${bin}${delimiter}${process.env.PATH}`, CARGO_CALLS_LOG: calls },
    encoding: 'utf8',
  });
  assert.ifError(result.error);
  return { ...result, calls: readFileSync(calls, 'utf8').trim().split('\n') };
}

test('qualification fails when target discovery fails', (t) => {
  const result = qualify(t, 'printf "%s\\n" first; echo "discovery failed" >&2; exit 42');
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /discovery failed/);
  assert.equal(result.calls.length, 1);
});

test('qualification rejects an empty target inventory', (t) => {
  const result = qualify(t, 'exit 0');
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /No harness fuzz targets found/);
  assert.equal(result.calls.length, 1);
});

test('qualification runs every discovered target', (t) => {
  const result = qualify(t, 'printf "%s\\n" first second');
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.calls.length, 3);
  assert.match(result.stdout, /FUZZ_PASS first /);
  assert.match(result.stdout, /FUZZ_PASS second /);
});
