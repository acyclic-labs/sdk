import {execFileSync} from 'node:child_process';
import fs from 'node:fs';
import {pathToFileURL} from 'node:url';

// Tests, fixtures, docs and retained evidence may name the disabled forms to
// prove the guard; every other tracked file must leave incremental on.
const excluded = file =>
  file.startsWith('managed-agents/') ||
  file.startsWith('docs/') ||
  /(^|\/)evidence\//.test(file) ||
  /(^|\/)fixtures\//.test(file) ||
  /(^|\/)[^/]*\.test\.[^/]+$/.test(file) ||
  /(^|\/)test-[^/]*$/.test(file);

const cargoFile = file =>
  file === 'Cargo.toml' || file.endsWith('/Cargo.toml') ||
  file === '.cargo/config.toml' || file.endsWith('/.cargo/config.toml');

// The variable name in any setting form: an assignment or [env] table entry,
// a quoted argument to .env(...)/set_var(...), or bracketed process.env access.
const name = String.raw`CARGO_(?:PROFILE_[A-Z0-9_]+_)?INCREMENTAL`;
const disabled = [
  [new RegExp(String.raw`\b${name}\b\s*[:=]\s*["']?(?:0|false)\b`, 'g'), 'sets CARGO_INCREMENTAL to 0'],
  [new RegExp(String.raw`["']${name}["']\s*[:=]\s*["']?(?:0|false)\b`, 'g'), 'sets CARGO_INCREMENTAL to 0'],
  [new RegExp(String.raw`\b${name}\b["']?\s*\]\s*[:=]\s*["']?(?:0|false)\b`, 'g'), 'sets CARGO_INCREMENTAL to 0'],
  [/-C\s*incremental\s*=\s*(?:no|off|false|0)\b/g, 'passes -C ' + 'incremental=no'],
];
// A quoted argument may put the value on a later line, as .env(...) calls do.
const quotedArgument = new RegExp(String.raw`["']${name}["']\s*,\s*["']?(?:0|false)\b`, 'g');
const strip = /-C\s*incremental/;
const stripper = /\b(?:sed|grep|awk|filter|replace|strip|remove)\b/;

export function violations(file, source) {
  if (excluded(file)) return [];
  const found = [];
  const lines = source.split('\n');
  if (cargoFile(file)) {
    for (const [index, line] of lines.entries()) {
      if (/\bincremental\s*=\s*["']?false\b/.test(line)) found.push(`${file}:${index + 1}: incremental = false`);
    }
  }
  const lineOf = index => source.slice(0, index).split('\n').length;
  for (const [pattern, what] of disabled) {
    for (const match of source.matchAll(pattern)) found.push(`${file}:${lineOf(match.index)}: ${what}`);
  }
  for (const match of source.matchAll(quotedArgument)) found.push(`${file}:${lineOf(match.index)}: sets CARGO_INCREMENTAL to 0`);
  for (const [index, line] of lines.entries()) {
    if (strip.test(line) && stripper.test(line)) found.push(`${file}:${index + 1}: strips -C ` + 'incremental');
  }
  return found;
}

export function checkIncremental(files = execFileSync('git', ['ls-files', '-z'], {encoding: 'utf8'}).split('\0').filter(Boolean)) {
  const found = [];
  for (const file of files) {
    if (excluded(file)) continue;
    found.push(...violations(file, fs.readFileSync(file, 'utf8')));
  }
  return found;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const found = checkIncremental();
  if (found.length) {
    console.error(found.join('\n'));
    process.exit(1);
  }
  console.log('Incremental compilation is enabled everywhere');
}
