import assert from 'node:assert/strict';
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

test('all public and internal library surfaces generate code separately with --all-features', () => {
  const selected = consumers(metadata);
  assert.deepEqual(selected.map(entry => entry.name),
    ['acyclic-harness-adapter', 'acyclic-harness-conformance', 'acyclic-public', 'registry-tool']);
  for (const entry of selected) {
    const base = ['build', '--locked', '--package', entry.name, '--all-features'];
    assert.deepEqual(commands(entry), entry.metadata.acyclic['consumer-examples'] ? [base, [...base, '--examples']] : [base]);
  }
  const required = item('required-consumer', { examples: true });
  assert.throws(() => commands({ ...required, targets: [{ kind: ['lib'] }] }), /unconditional executable/);
  assert.throws(() => commands({ ...required, targets: [
    { kind: ['example'], 'required-features': ['hidden-feature'] },
  ] }), /unconditional executable/);
});
