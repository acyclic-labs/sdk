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
export function parseToml(texts) {
  return JSON.parse(execFileSync(process.platform === 'win32' ? 'python' : 'python3', ['-I', '-c', `
import json, sys, tomllib
documents = {}
for path, text in json.load(sys.stdin).items():
    try:
        documents[path] = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        sys.exit(f"{path}: {error}")
json.dump(documents, sys.stdout)
`], {input: JSON.stringify(texts), encoding: 'utf8', maxBuffer: Infinity}));
}

export function toml(root, file) {
  const key = resolve(root, file);
  if (!manifests.has(key)) manifests.set(key, parseToml({[file]: readFileSync(key, 'utf8')})[file]);
  return manifests.get(key);
}

// Expectations name actual parsed fields, independently of manifest formatting.
export function checkManifestFields(root, expectations) {
  const files = [...new Set(expectations.map(([file]) => file))];
  const parsed = parseToml(Object.fromEntries(files.filter(file => file.endsWith('.toml')).map(file => [file, readFileSync(resolve(root, file), 'utf8')])));
  for (const file of files.filter(file => !file.endsWith('.toml'))) parsed[file] = JSON.parse(readFileSync(resolve(root, file), 'utf8'));
  for (const [file, keys, expected] of expectations) {
    const actual = keys.reduce((value, key) => value?.[key], parsed[file]);
    if (actual !== expected) throw new Error(`${file}: ${keys.join('.')} must be ${JSON.stringify(expected)}; found ${JSON.stringify(actual)}`);
  }
}
