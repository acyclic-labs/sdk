import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {posix} from 'node:path';
import {expandFuzzIncludes, fuzzOracleViolations, rustTokens} from './fuzz-oracle-policy.mjs';

const [ref, ...extra] = process.argv.slice(2);
if (extra.length) throw new Error('usage: check-fuzz-oracles.mjs [git-ref]');
const git = (...args) => execFileSync('git', args, {encoding: 'utf8', maxBuffer: 32 * 1024 * 1024});
const commit = ref && git('rev-parse', '--verify', '--end-of-options', `${ref}^{commit}`).trim();
const tracked = new Set((commit ? git('ls-tree', '-r', '-z', '--name-only', commit) : git('ls-files', '-z')).split('\0').filter(Boolean));
const paths = [...tracked].filter(path => /(?:^|\/)fuzz_targets\/.*\.rs$/.test(path));
const readSources = names => {
  if (!commit) return names.map(path => readFileSync(path, 'utf8'));
  const output = execFileSync('git', ['cat-file', '--batch'], {
    input: names.map(path => `${commit}:${path}\n`).join(''), maxBuffer: 128 * 1024 * 1024,
  });
  let offset = 0;
  return names.map(() => {
    const end = output.indexOf(10, offset), header = output.subarray(offset, end).toString();
    const match = /^[0-9a-f]+ blob (\d+)$/.exec(header);
    if (end < 0 || !match) throw new Error('could not read exact Git source blob: ' + header);
    const start = end + 1, size = Number(match[1]);
    offset = start + size + 1;
    return output.subarray(start, start + size).toString('utf8');
  });
};
const rawSources = readSources(paths);
const raw = new Map(paths.map((path, index) => [path, rawSources[index]]));
const load = path => {
  if (!tracked.has(path)) throw new Error('include is outside the tracked source tree: ' + path);
  if (!raw.has(path)) raw.set(path, readSources([path])[0]);
  return raw.get(path);
};
const resolveInclude = (file, reference, manifest) => {
  let directory = posix.dirname(file);
  if (manifest) {
    while (!tracked.has(posix.join(directory, 'Cargo.toml'))) {
      const parent = posix.dirname(directory);
      if (parent === directory) throw new Error('cannot resolve Cargo manifest for ' + file);
      directory = parent;
    }
    if (!reference.startsWith('/')) throw new Error('non-relative manifest include');
    reference = reference.slice(1);
  } else if (posix.isAbsolute(reference) || reference.includes('\\')) throw new Error('non-relative Rust include');
  const path = posix.normalize(posix.join(directory, reference));
  if (path.startsWith('../') || !path.endsWith('.rs')) throw new Error('invalid Rust include: ' + path);
  return path;
};
const shared = 'rust/fuzz/support/round_trip.rs';
const nonCodecModules = new Map([
  ['file', 'rust/crates/harness/src/client/diff/file.rs'],
  ['local_case', 'rust/crates/harness/tests/e2e/local_case.rs'],
  ['executable', 'rust/crates/harness/src/mcp/install/executable.rs'],
]);
const checkNonCodecModules = (file, source) => {
  const tokens = rustTokens(source);
  for (const [name, expected] of nonCodecModules) {
    if (!tokens.some((t, i) => t.text === 'crate' && tokens[i + 1]?.text === '::' && tokens[i + 2]?.text === name)) continue;
    let found = false;
    for (let i = 0; i < tokens.length; i++) if (tokens[i].text === 'mod' && tokens[i + 1]?.text === name) {
      let end = i;
      if (tokens[end - 1]?.text === 'pub') end--;
      const attr = tokens.slice(end - 6, end);
      if (tokens[i + 2]?.text !== ';' || attr.map(t => t.text).join(' ') !== '# [ path = <literal> ]'
        || posix.normalize(posix.join(posix.dirname(file), JSON.parse(attr[4].raw))) !== expected)
        throw new Error('non-codec module must use its canonical production/fixture path: ' + name);
      found = true;
    }
    if (!found) throw new Error('missing canonical non-codec module: ' + name);
  }
};
const trustedSupport = (file, seen = new Set()) => {
  if (seen.has(file)) return false;
  seen.add(file);
  const tokens = rustTokens(load(file));
  for (let i = 0; i < tokens.length; i++) {
    if (tokens.slice(i, i + 5).map(t => t.text).join(' ') === 'use acyclic_fuzz_support as fuzz_support ;') {
      let directory = posix.dirname(file);
      while (!tracked.has(posix.join(directory, 'Cargo.toml'))) {
        const parent = posix.dirname(directory);
        if (parent === directory) throw new Error('missing helper dependency manifest');
        directory = parent;
      }
      const manifest = load(posix.join(directory, 'Cargo.toml'));
      const dependency = /^acyclic-fuzz-support\s*=\s*\{\s*path\s*=\s*"([^"]+)"\s*\}/m.exec(manifest);
      if (!dependency || posix.normalize(posix.join(directory, dependency[1])) !== posix.dirname(shared))
        throw new Error('fuzz_support must depend on the canonical shared helper crate');
      return tracked.has(shared);
    }
    if (['goal_parser_input', 'recorded_invocation_input'].includes(tokens[i + 1]?.text)
      && tokens.slice(i, i + 5).map(t => t.text).join(' ') === `use ${tokens[i + 1].text} :: fuzz_support ;`)
      return trustedSupport(posix.join(posix.dirname(file), tokens[i + 1].text + '.rs'), seen);
  }
  return false;
};
// Production input factories belong only to the oracle. The test-only factory
// may be called directly by the four existing ordinary test consumers; it may
// never be aliased or re-exported into a fuzz target's dependency graph.
const inputTests = new Set([
  'rust/crates/harness/src/projection/recorded_invocation_tests.rs',
  'rust/crates/harness/tests/goal_files.rs',
  'rust/crates/stream/tests/checked_session_contract.rs',
  'rust/crates/test-support/src/fuzz_round_trip.rs',
]);
let factories = '';
try { factories = git('grep', '-l', '-w', '-E', '__acyclic_(fuzz_entry|parsed_input)|test_input',
  ...(commit ? [commit] : []), '--', '*.rs'); }
catch (error) { if (error.status !== 1) throw error; }
const factoryFiles = factories.split('\n').filter(Boolean)
  .map(path => commit ? path.slice(commit.length + 1) : path);
for (const path of factoryFiles) if (path !== shared) {
  const tokens = rustTokens(load(path));
  if (!inputTests.has(path) || tokens.some((token, i) => token.text.startsWith('__acyclic_')
    || token.text === 'test_input' && tokens[i + 1]?.text !== '('))
    throw new Error('raw fuzz-input factory outside shared oracle: ' + path);
}
const sources = paths.map(path => expandFuzzIncludes(path, load(path), resolveInclude, load));
const errors = [];
for (let index = 0; index < paths.length; index++) {
  const path = paths[index];
  try {
    checkNonCodecModules(path, load(path));
    if (rustTokens(sources[index]).some(t => t.text === 'fuzz_support') && !trustedSupport(path))
      throw new Error('missing canonical shared oracle module');
    errors.push(...fuzzOracleViolations(path, sources[index]));
  } catch (error) {
    errors.push(`${path}: fuzz round-trip oracle analysis failed closed: ${error.message}`);
  }
}
if (!paths.length) errors.push('no Rust fuzz targets found');
if (errors.length) { console.error(errors.join('\n')); process.exitCode = 1; }
else console.log(`Fuzz round-trip oracle lint passed (${paths.length} Rust sources${commit ? ` at ${commit}` : ''})`);
