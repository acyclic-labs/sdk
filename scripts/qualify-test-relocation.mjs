#!/usr/bin/env node
// Run with exclusive Cargo artifact custody in a disposable checkout.
// Keep the original CLI oracle and native owner.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { stageArtifacts } from './graphcoder-e2e/artifacts.mjs';
import { testExecutable, verify } from './qualify-main-green-debts.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const test = 'runner_cli::cli_accepts_stdin_and_rejects_a_failed_report_from_a_path';
const entry = { package: 'acyclic-conformance', target: 'main', regression: test, controls: [test] };
const digest = bytes => createHash('sha256').update(bytes).digest('hex');

async function main() {
  assert.equal(process.argv.length, 3, 'expected a fresh evidence directory');
  const output = resolve(process.argv[2]);
  await mkdir(output);
  const source = join(root, 'rust/crates/test-support/src/fixtures.rs');
  const original = await readFile(source);
  const oracle = await readFile(join(root, 'rust/crates/conformance/tests/runner_cli.rs'));
  const needle = `let selected = ($selected)
            .or_else(|| ::std::env::var_os(concat!("CARGO_BIN_EXE_", $name)))
            .or_else(|| ::std::env::var_os(concat!("NEXTEST_BIN_EXE_", $name).replace('-', "_")));`;
  const text = original.toString('utf8').replaceAll('\r\n', '\n');
  assert.equal(text.split(needle).length, 2, 'runtime selection source anchor must be unique');
  const report = { source_sha256: digest(original), oracle_sha256: digest(oracle), steps: [] };

  async function phase(name, negative) {
    const directory = join(output, name);
    await mkdir(directory);
    const build = spawnSync('cargo', ['test', '--locked', '-p', entry.package, '--test', entry.target,
      '--no-run', '--message-format=json-render-diagnostics'], { cwd: root, encoding: 'utf8', maxBuffer: Infinity });
    await writeFile(join(directory, 'cargo.jsonl'), build.stdout ?? '');
    await writeFile(join(directory, 'compiler.log'), build.stderr ?? '');
    process.stderr.write(build.stderr ?? '');
    if (build.error) throw build.error;
    assert.equal(build.status, 0, `${name}: original consumer must compile`);
    const executable = testExecutable(entry, build.stdout);
    const { artifacts } = await stageArtifacts([{ name: `harness-conformance${process.platform === 'win32' ? '.exe' : ''}`,
      target: 'harness-conformance', test: false, records: build.stdout }], join(directory, 'artifacts'));
    const artifact = Object.values(artifacts)[0];
    const held = `${artifact.source}.relocation-held`;

    async function run(mode) {
      const env = { ...process.env };
      for (const key of ['CARGO_BIN_EXE_harness-conformance', 'NEXTEST_BIN_EXE_harness-conformance',
        'NEXTEST_BIN_EXE_harness_conformance']) delete env[key];
      if (mode === 'cargo') {
        env['CARGO_BIN_EXE_harness-conformance'] = artifact.path;
        env.NEXTEST_BIN_EXE_harness_conformance = join(directory, 'unselected');
      } else if (mode === 'nextest') env.NEXTEST_BIN_EXE_harness_conformance = artifact.path;
      const result = spawnSync(executable, [test, '--exact', '--nocapture', '--color=never'],
        { cwd: root, env, encoding: 'utf8', maxBuffer: Infinity });
      if (result.error) throw result.error;
      const log = (result.stdout ?? '') + (result.stderr ?? '');
      await writeFile(join(directory, `${mode}.log`), log);
      process.stdout.write(log);
      report.steps.push({ name, mode, exit: result.status, log_sha256: digest(log),
        test_executable: executable, cargo_json_sha256: digest(build.stdout), artifact });
      await writeFile(join(output, 'receipt.json'), JSON.stringify(report, null, 2) + '\n');
      if (negative) {
        assert.notEqual(result.status, 0, 'omitting runtime metadata must fail the original CLI test');
        assert.match(log, /test result: FAILED\. 0 passed; 1 failed; 0 ignored;/);
        assert.match(log, /code: 2\b|os error 2\)/, 'failure must be the absent original executable');
      } else verify(entry, result.status, result.stdout);
    }

    // The copied bytes retain the original Cargo identity and digest. Making
    // the compiled location absent forces the same test to use runtime metadata.
    await rename(artifact.source, held);
    try {
      await run('cargo');
      if (!negative) await run('nextest');
    } finally {
      await rename(held, artifact.source);
    }
    if (!negative) await run('compiled');
  }

  try {
    await writeFile(source, text.replace(needle, 'let selected = ($selected);'));
    await phase('runtime-lookup-omitted', true);
  } finally {
    await writeFile(source, original);
  }
  await phase('restored', false);
  assert.deepEqual(await readFile(source), original, 'source restoration changed bytes');
  assert.deepEqual(await readFile(join(root, 'rust/crates/conformance/tests/runner_cli.rs')), oracle,
    'original CLI oracle changed');
  console.log(`TEST_RELOCATION_QUALIFIED ${join(output, 'receipt.json')}`);
}

main().catch(error => {
  const recovery = [process.execPath, fileURLToPath(import.meta.url),
    resolve(`${process.argv[2] ?? 'test-relocation'}-corrected`)];
  console.error(`STEP_FAILED test relocation: ${error.stack ?? error}; correct the failing source or command, then recovery command: ${recovery.map(JSON.stringify).join(' ')}`);
  process.exitCode = 1;
});
