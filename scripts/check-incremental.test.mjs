import test from 'node:test';
import assert from 'node:assert/strict';
import {checkIncremental, violations} from './check-incremental.mjs';

test('the repository enables incremental compilation everywhere', () => {
  assert.deepEqual(checkIncremental(), []);
});

test('the check refuses every way incremental compilation is disabled', () => {
  const cases = [
    ['Cargo.toml', '[profile.release]\nincremental = false\n'],
    ['.cargo/config.toml', '[profile.dev]\nincremental = false\n'],
    ['scripts/build.sh', 'export CARGO_INCREMENTAL=0\n'],
    ['rust/crates/sdk-generation/tests/cli_regressions/runtime.rs', '        .env("CARGO_PROFILE_DEV_INCREMENTAL", "false")\n'],
    ['rust/src/main.rs', 'env::set_var("CARGO_INCREMENTAL", "0");\n'],
    ['rust/src/main.rs', 'process.env["CARGO_INCREMENTAL"] = "0";\n'],
    ['.github/workflows/x.yml', 'env:\n  CARGO_INCREMENTAL: 0\n'],
    ['scripts/build.mjs', 'process.env.CARGO_INCREMENTAL = "0";\n'],
    ['scripts/build.sh', 'RUSTFLAGS=$(echo "$RUSTFLAGS" | sed "s/-C incremental//")\n'],
  ];
  for (const [file, source] of cases) assert.notDeepEqual(violations(file, source), [], file);
});

test('the check accepts incremental compilation left on', () => {
  assert.deepEqual(violations('Cargo.toml', '[profile.release]\nincremental = true\n'), []);
  assert.deepEqual(violations('scripts/build.sh', 'export CARGO_INCREMENTAL=1\n'), []);
});
