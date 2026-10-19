import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const manifest = 'rust/crates/harness/tests/ui/native-environment/Cargo.toml';
export function hermeticCompilerPlans() {
  return [false, true].map(negative => ({negative, args: ['clippy', '--locked', '--manifest-path', manifest,
    '--lib', '--message-format=json', ...(negative ? ['--features', 'negative'] : []),
    '--', '-D', 'clippy::disallowed_methods', '-D', 'clippy::disallowed_macros']}));
}

export function qualifyHermeticCompiler(run = spawnSync) {
for (const {negative, args} of hermeticCompilerPlans()) {
  const start = performance.now();
  const result = run('cargo', args, {cwd: root, encoding: 'utf8', maxBuffer: 1 << 23});
  if (result.error) throw result.error;
  const diagnostics = result.stdout.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line))
    .filter(record => record.reason === 'compiler-message').map(record => record.message);
  const errors = diagnostics.filter(message => message.level === 'error');
  if (!negative) assert.equal(result.status, 0, result.stderr + errors.map(error => error.rendered).join(''));
  else {
    assert.notEqual(result.status, 0, 'negative control unexpectedly compiled');
    assert.equal(errors.length, 11, result.stderr + errors.map(error => error.rendered).join(''));
    assert.ok(errors.every(error => ['clippy::disallowed_methods', 'clippy::disallowed_macros'].includes(error.code?.code)), 'controls failed for an unrelated compiler error');
    assert.equal(new Set(errors.flatMap(error => error.spans.filter(span => span.is_primary).map(span => span.line_start))).size, 11);
  }
  console.log(`${negative ? 'negative (11 compiler-resolved rejections)' : 'positive (real seam)'}: ${(performance.now() - start).toFixed(0)} ms`);
}

}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) qualifyHermeticCompiler();
