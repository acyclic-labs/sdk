import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {toml, trackedFiles} from '../policy-core.mjs';

test('Git inventory preserves path bytes, pathspecs, and missing tracked files', t => {
  const root = mkdtempSync(join(tmpdir(), 'policy-core-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  execFileSync('git', ['init', '-q', root]);
  const paths = ['space name.rs', 'quote"name.rs', 'line\nbreak.rs', 'tab\tname.rs', 'unicodé.rs', 'Cargo.toml'];
  for (const path of paths) writeFileSync(join(root, path), '');
  writeFileSync(join(root, 'Cargo.toml'), '[package]\nname = "literal#value"\nedition = "2024"\n');
  execFileSync('git', ['add', '.'], {cwd: root});
  rmSync(join(root, paths[0]));
  writeFileSync(join(root, 'untracked.rs'), '');
  assert.deepEqual(trackedFiles(root).sort(), paths.sort());
  assert.deepEqual(trackedFiles(root, ['*.rs', ':!tab*']).sort(), paths.filter(path => path.endsWith('.rs') && !path.startsWith('tab')));
  assert.equal(toml(root, 'Cargo.toml').package.name, 'literal#value');
  assert.throws(() => toml(root, 'absent.toml'));
});
