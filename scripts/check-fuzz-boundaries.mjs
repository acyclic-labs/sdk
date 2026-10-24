import { readFileSync, readdirSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

// Tokenize only the Rust surface needed by this policy. Comments and string
// contents cannot manufacture imports; cfg_attr, raw strings, and spacing are
// handled exactly like ordinary attributes/macros.
function tokens(source) {
  const result = [];
  let i = 0;
  while (i < source.length) {
    if (/\s/.test(source[i])) { i++; continue; }
    if (source.startsWith('//', i)) {
      const end = source.indexOf('\n', i + 2);
      i = end < 0 ? source.length : end;
      continue;
    }
    if (source.startsWith('/*', i)) {
      let depth = 1;
      i += 2;
      while (i < source.length && depth) {
        if (source.startsWith('/*', i)) { depth++; i += 2; }
        else if (source.startsWith('*/', i)) { depth--; i += 2; }
        else i++;
      }
      continue;
    }
    const start = i;
    const raw = /^(?:br|cr|r)(#*)"/.exec(source.slice(i));
    if (raw) {
      const endMarker = `"${raw[1]}`;
      const content = i + raw[0].length;
      const end = source.indexOf(endMarker, content);
      result.push({value: source.slice(content, end < 0 ? source.length : end), string: true, start});
      i = end < 0 ? source.length : end + endMarker.length;
      continue;
    }
    if (source[i] === '"') {
      i++;
      let value = '';
      while (i < source.length && source[i] !== '"') {
        if (source[i] !== '\\') { value += source[i++]; continue; }
        const escape = /^\\(?:x([\da-fA-F]{2})|u\{([\da-fA-F_]+)\}|(\r?\n)\s*|(.))/s.exec(source.slice(i));
        if (!escape) { i++; continue; }
        value += escape[1] ? String.fromCodePoint(parseInt(escape[1], 16))
          : escape[2] ? String.fromCodePoint(parseInt(escape[2].replaceAll('_', ''), 16))
          : escape[3] ? '' : ({n: '\n', r: '\r', t: '\t', '0': '\0'}[escape[4]] ?? escape[4]);
        i += escape[0].length;
      }
      i++;
      result.push({value, string: true, start});
      continue;
    }
    // Character literals are opaque, while lifetimes remain ordinary tokens.
    const character = /^'(?:\\(?:u\{[\da-fA-F_]+\}|x[\da-fA-F]{2}|.)|[^'\\\r\n])'/su.exec(source.slice(i));
    if (character) { i += character[0].length; continue; }
    const identifier = /^(?:r#)?[A-Za-z_][A-Za-z_0-9]*/.exec(source.slice(i));
    const value = identifier ? identifier[0] : source[i];
    result.push({value: value.replace(/^r#/, ''), start});
    i += value.length;
  }
  return result;
}

export function violations(source, filename, packageRoot) {
  const problems = [];
  const harnessFuzz = packageRoot.split(sep).at(-1) === 'harness-fuzz';
  const input = tokens(source);
  let attributeDepth = 0;
  for (let i = 0; i < input.length; i++) {
    const token = input[i];
    if (token.string) continue;
    if (token.value === '[' && (attributeDepth || input[i - 1]?.value === '#' ||
        (input[i - 1]?.value === '!' && input[i - 2]?.value === '#'))) attributeDepth++;
    if (token.value === ']') attributeDepth = Math.max(0, attributeDepth - 1);
    const line = () => source.slice(0, token.start).split('\n').length;
    if (attributeDepth && token.value === 'path' && input[i + 1]?.value === '=' && input[i + 2]?.string) {
      const path = input[i + 2].value;
      const target = resolve(dirname(filename), path);
      const within = relative(packageRoot, target);
      if (harnessFuzz || within === '..' || within.startsWith(`..${sep}`) || isAbsolute(within)) {
        problems.push(`${filename}:${line()}: import ${path} through the owning crate's API`);
      }
    }
    if (token.value === 'include' && input[i + 1]?.value === '!') {
      problems.push(`${filename}:${line()}: compile shared code through the owning crate instead of include!`);
    }
  }
  return problems;
}

export function check(root) {
  const problems = [];
  function visit(directory, fuzzPackage) {
    const entries = readdirSync(directory, { withFileTypes: true });
    const manifest = entries.find(entry => entry.name === 'Cargo.toml');
    if (manifest) {
      fuzzPackage = /cargo-fuzz\s*=\s*true/.test(readFileSync(join(directory, manifest.name), 'utf8'))
        ? directory : undefined;
    }
    for (const entry of entries) {
      const filename = join(directory, entry.name);
      if (entry.isDirectory() && !['target', 'node_modules', '.git'].includes(entry.name)) visit(filename, fuzzPackage);
      else if (entry.isFile() && entry.name.endsWith('.rs') && fuzzPackage) {
        problems.push(...violations(readFileSync(filename, 'utf8'), filename, fuzzPackage));
      }
    }
  }
  visit(resolve(root), undefined);
  return problems;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const problems = check(resolve(dirname(fileURLToPath(import.meta.url)), '..'));
  if (problems.length) {
    process.stderr.write(`${problems.join('\n')}\n`);
    process.exitCode = 1;
  } else process.stdout.write('All Rust fuzz packages use their owning crates for external code.\n');
}
