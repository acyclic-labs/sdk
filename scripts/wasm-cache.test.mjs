import assert from 'node:assert/strict';
import test from 'node:test';
import { overrides } from './wasm-cache.mjs';

test('compiler, flag and bindgen overrides bypass the cache; wrappers and empty values do not', () => {
  assert.deepEqual(overrides({
    RUSTFLAGS: '-C x', CARGO_ENCODED_RUSTFLAGS: 'a', RUSTC: '/x/rustc', ACYCLIC_CARGO_BIN: 'c',
    ACYCLIC_WASM_BINDGEN_BIN: '', CARGO_PROFILE_WASM_RELEASE_LTO: 'off',
    RUSTC_WRAPPER: 'sccache', CARGO_HOME: '/c', PATH: '/bin',
  }), ['ACYCLIC_CARGO_BIN', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_PROFILE_WASM_RELEASE_LTO', 'RUSTC', 'RUSTFLAGS']);
  assert.deepEqual(overrides({ PATH: '/bin' }), []);
  // bb2 exports dev/test profile settings for every job; they never reach wasm-release.
  assert.deepEqual(overrides({ CARGO_PROFILE_DEV_DEBUG: '0', CARGO_PROFILE_TEST_INCREMENTAL: 'false' }), []);
  assert.deepEqual(overrides({ ACYCLIC_WASM_PROFILE: 'dev', CARGO_PROFILE_DEV_DEBUG: '0' }), ['CARGO_PROFILE_DEV_DEBUG']);
});
