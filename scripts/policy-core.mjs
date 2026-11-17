import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

export const isMain = url => Boolean(process.argv[1]) && resolve(process.argv[1]) === fileURLToPath(url);

export const repositoryUrl = new URL('../', import.meta.url).href;
export const repositoryRoot = resolve(fileURLToPath(repositoryUrl));

// Git paths are NUL-delimited, including names with newlines or quoting characters.
// Each policy owns its pathspecs; discovery never silently drops missing files.
/** @param {string} [root] @param {string[]} [paths] @param {import('node:child_process').ExecFileSyncOptions & {untracked?: boolean}} [options] */
export function trackedFiles(root = process.cwd(), paths = [], {untracked = false, ...options} = {}) {
  return execFileSync('git', ['ls-files', '-z', ...(untracked ? ['--cached', '--others', '--exclude-standard'] : []), '--', ...paths],
    {...options, cwd: root, encoding: 'utf8', maxBuffer: Infinity}).split('\0').filter(Boolean);
}

const manifests = new Map();
export function parseToml(texts) {
  return JSON.parse(execFileSync(process.platform === 'win32' ? 'python' : 'python3', ['-I', '-c', `
import datetime, json, math, sys, tomllib
def encode(value):
    if isinstance(value, dict):
        return {key: encode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode(item) for item in value]
    if isinstance(value, (datetime.date, datetime.time)):
        return {"toml_type": type(value).__name__, "value": value.isoformat()}
    if isinstance(value, float) and not math.isfinite(value):
        return {"toml_type": "float", "value": str(value)}
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 9007199254740991:
        return {"toml_type": "integer", "value": str(value)}
    return value
documents = {}
for path, text in json.load(sys.stdin).items():
    try:
        documents[path] = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        sys.exit(f"{path}: {error}")
json.dump(encode(documents), sys.stdout, allow_nan=False)
`], {input: JSON.stringify(texts), encoding: 'utf8', maxBuffer: Infinity}));
}

export function tomlFiles(root, files) {
  const missing = files.filter(file => !manifests.has(resolve(root, file)));
  if (missing.length) {
    const parsed = parseToml(Object.fromEntries(missing.map(file => [file, readFileSync(resolve(root, file), 'utf8')])));
    for (const [file, manifest] of Object.entries(parsed)) manifests.set(resolve(root, file), manifest);
  }
  return Object.fromEntries(files.map(file => [file, manifests.get(resolve(root, file))]));
}

export const toml = (root, file) => tomlFiles(root, [file])[file];

// Expectations name actual parsed fields, independently of manifest formatting.
export function checkManifestFields(root, expectations) {
  const files = [...new Set(expectations.map(([file]) => file))];
  const parsed = tomlFiles(root, files.filter(file => file.endsWith('.toml')));
  for (const file of files.filter(file => !file.endsWith('.toml'))) parsed[file] = JSON.parse(readFileSync(resolve(root, file), 'utf8'));
  for (const [file, keys, expected] of expectations) {
    const actual = keys.reduce((value, key) => value?.[key], parsed[file]);
    if (actual !== expected) throw new Error(`${file}: ${keys.join('.')} must be ${JSON.stringify(expected)}; found ${JSON.stringify(actual)}`);
  }
}
