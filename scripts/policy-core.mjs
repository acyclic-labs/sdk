import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

export const isMain = url => Boolean(process.argv[1]) && resolve(process.argv[1]) === fileURLToPath(url);

// Git paths are NUL-delimited, including names with newlines or quoting characters.
// Each policy owns its pathspecs; discovery never silently drops missing files.
export function trackedFiles(root = process.cwd(), paths = [], options = {}) {
  return execFileSync('git', ['ls-files', '-z', '--', ...paths],
    {...options, cwd: root, encoding: 'utf8', maxBuffer: Infinity}).split('\0').filter(Boolean);
}

const manifests = new Map();
export function toml(root, file) {
  const key = resolve(root, file);
  if (!manifests.has(key)) manifests.set(key, JSON.parse(execFileSync('python3', ['-I', '-c',
    "import json, sys, tomllib; json.dump(tomllib.load(open(sys.argv[1], 'rb')), sys.stdout)", key], {encoding: 'utf8'})));
  return manifests.get(key);
}

// Expectations name actual parsed fields, independently of manifest formatting.
export function checkManifestFields(root, expectations) {
  for (const [file, keys, expected] of expectations) {
    const document = file.endsWith('.toml') ? toml(root, file) : JSON.parse(readFileSync(resolve(root, file), 'utf8'));
    const actual = keys.reduce((value, key) => value?.[key], document);
    if (actual !== expected) throw new Error(`${file}: ${keys.join('.')} must be ${JSON.stringify(expected)}; found ${JSON.stringify(actual)}`);
  }
}
