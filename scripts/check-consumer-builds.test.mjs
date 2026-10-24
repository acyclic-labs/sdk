import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { consumers, commands } from './check-consumer-builds.mjs';

function item(name, { publish = [], kind = 'lib', fuzz = false, examples = false } = {}) {
  return { id: name, name, publish, metadata: { 'cargo-fuzz': fuzz, acyclic: { 'consumer-examples': examples } },
    targets: [{ kind: [kind] }, ...(examples ? [{ kind: ['example'], name: 'consumer' }] : [])] };
}
const packages = [item('acyclic-public', { publish: null }), item('acyclic-harness-conformance', { examples: true }),
  item('acyclic-harness-adapter'), item('fixture-tool', { kind: 'bin' }), item('registry-tool', { publish: ['internal'], kind: 'bin' }),
  item('acyclic-fuzz', { kind: 'bin', fuzz: true }), item('registry-library')];
const metadata = { workspace_members: packages.slice(0, -1).map(entry => entry.id), packages };

test('all public and internal library surfaces generate code separately in both feature selections', () => {
  const selected = consumers(metadata);
  assert.deepEqual(selected.map(entry => entry.name),
    ['acyclic-harness-adapter', 'acyclic-harness-conformance', 'acyclic-public', 'registry-tool']);
  for (const entry of selected) {
    const base = ['build', '--locked', '--package', entry.name];
    assert.deepEqual(commands(entry), entry.metadata.acyclic['consumer-examples'] ? [
      base, [...base, '--examples'], [...base, '--all-features'], [...base, '--all-features', '--examples'],
    ] : [base, [...base, '--all-features']]);
  }
  const required = item('required-consumer', { examples: true });
  assert.throws(() => commands({ ...required, targets: [{ kind: ['lib'] }] }), /unconditional executable/);
  assert.throws(() => commands({ ...required, targets: [
    { kind: ['example'], 'required-features': ['hidden-feature'] },
  ] }), /unconditional executable/);
});

test('shared gate rejects codegen-only failures and scopes PR consumers; old check-only hook admits the negative', () => {
  const cwd = mkdtempSync(join(tmpdir(), 'consumer-codegen-gate-'));
  try {
    mkdirSync(join(cwd, 'scripts')); mkdirSync(join(cwd, 'bin'));
    writeFileSync(join(cwd, 'metadata.json'), JSON.stringify(metadata));
    writeFileSync(join(cwd, 'scripts/check-consumer-builds.mjs'),
      readFileSync(new URL('./check-consumer-builds.mjs', import.meta.url)));
    writeFileSync(join(cwd, 'feature.mjs'), 'process.exit(Number(process.env.FEATURE_RC ?? 0));\n');
    const cargo = join(cwd, 'bin/cargo');
    writeFileSync(cargo, `#!/usr/bin/env node
const fs=require('node:fs'), args=process.argv.slice(2);
fs.appendFileSync('calls',JSON.stringify(args)+'\\n');
if(args[0]==='metadata') console.log(fs.readFileSync('metadata.json','utf8'));
else if(args[0]==='build' && args.includes('acyclic-harness-conformance') && process.env.REJECT_LAYOUT==='1') {
 console.error('error: queries overflow the depth limit!'); process.exitCode=101;
}
`); chmodSync(cargo, 0o755);
    const hook = readFileSync(new URL('./gate-feature-matrix.sh', import.meta.url), 'utf8');
    const payload = readFileSync(new URL('./gate-feature-payload.sh', import.meta.url), 'utf8');
    const old = payload.replace(/^node scripts\/check-consumer-builds\.mjs.*$/m, 'consumer_rc=0');
    assert.notEqual(old, payload);
    writeFileSync(join(cwd, 'scripts/gate-target-cache.py'), readFileSync(new URL('./gate-target-cache.py', import.meta.url)));
    for (const { negative, reject, affected, feature, expected } of [
      { reject: true, expected: 1 }, { negative: true, reject: true, expected: 0 },
      { expected: 0 }, { affected: true, expected: 0 }, { feature: 3, expected: 3 },
    ]) {
      writeFileSync(join(cwd, 'runner.sh'), hook);
      writeFileSync(join(cwd, 'scripts/gate-feature-payload.sh'), negative ? old : payload);
      writeFileSync(join(cwd, 'calls'), '');
      writeFileSync(join(cwd, '.gate-scope-mode'), affected ? 'affected\n' : 'full\n');
      writeFileSync(join(cwd, '.gate-clippy-packages'), 'acyclic-harness-conformance\nfixture-tool\n');
      const result = spawnSync('bash', ['runner.sh'], { cwd, encoding: 'utf8', env: {
        ...process.env, PATH: join(cwd, 'bin') + ':' + process.env.PATH,
        GATE_FEATURE_TARGET_DIR: join(cwd, 'target'), GATE_FEATURE_WORKERS: '1',
        GATE_FEATURE_SCRIPT: 'feature.mjs', REJECT_LAYOUT: reject ? '1' : '0', FEATURE_RC: String(feature ?? 0),
      } });
      assert.equal(result.status, expected, result.stdout + result.stderr);
      const builds = readFileSync(join(cwd, 'calls'), 'utf8').split('\n').filter(Boolean)
        .map(line => JSON.parse(line)).filter(args => args[0] === 'build');
      assert.equal(builds.length, negative ? 0 : affected ? 4 : 10);
      if (affected) assert.ok(builds.every(args => args.includes('acyclic-harness-conformance')));
      if (reject && !negative) assert.match(result.stderr, /queries overflow the depth limit/);
      if (!negative) assert.match(result.stdout, /consumer builds: \d+\/\d+ sets ok/);
    }
  } finally { rmSync(cwd, { recursive: true, force: true }); }
});
