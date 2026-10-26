import assert from 'node:assert/strict';
import test from 'node:test';
import { overrides, restamp } from './wasm-cache.mjs';

test('compiler, workspace adapter, flag and bindgen overrides bypass the cache', () => {
  assert.deepEqual(overrides({
    RUSTFLAGS: '-C x', CARGO_ENCODED_RUSTFLAGS: 'a', RUSTC: '/x/rustc', ACYCLIC_CARGO_BIN: 'c',
    ACYCLIC_WASM_BINDGEN_BIN: '', CARGO_PROFILE_WASM_RELEASE_LTO: 'off',
    RUSTC_WRAPPER: 'sccache', RUSTC_WORKSPACE_WRAPPER: '/custom/adapter', CARGO_HOME: '/c', PATH: '/bin',
  }), ['ACYCLIC_CARGO_BIN', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_PROFILE_WASM_RELEASE_LTO', 'RUSTC', 'RUSTC_WORKSPACE_WRAPPER', 'RUSTFLAGS']);
  assert.deepEqual(overrides({ PATH: '/bin', RUSTC_WORKSPACE_WRAPPER: '' }), []);
  // bb2 exports dev/test profile settings for every job; they never reach wasm-release.
  assert.deepEqual(overrides({ CARGO_PROFILE_DEV_DEBUG: '0', CARGO_PROFILE_TEST_INCREMENTAL: 'false' }), []);
  assert.deepEqual(overrides({ ACYCLIC_WASM_PROFILE: 'dev', CARGO_PROFILE_DEV_DEBUG: '0' }), ['CARGO_PROFILE_DEV_DEBUG']);
});

test('a hit binds the producer receipt to HEAD and leaves other outputs untouched', () => {
  const head = 'a'.repeat(40);
  const receipt = Buffer.from(`${JSON.stringify({ schema: 'acyclic.wasm-build-receipt.v1', source_commit: 'b'.repeat(40), source_sha256: 'x' }, null, 2)}\n`);
  const out = JSON.parse(restamp('typescript/packages/actors/generated/wasm/producer-receipt.json', receipt, head).toString());
  assert.deepEqual(out, { schema: 'acyclic.wasm-build-receipt.v1', source_commit: head, source_sha256: 'x' });
  const other = Buffer.from('{"source_commit":"b"}');
  assert.equal(restamp('typescript/packages/actors/generated/wasm/index.js', other, head), other);
  assert.throws(() => restamp('x/producer-receipt.json', Buffer.from('{"schema":"other"}'), head));
});
