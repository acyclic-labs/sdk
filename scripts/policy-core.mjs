import {execFileSync} from 'node:child_process';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

export const isMain = url => Boolean(process.argv[1]) && resolve(process.argv[1]) === fileURLToPath(url);

// Git paths are NUL-delimited, including names with newlines or quoting characters.
// Each policy owns its pathspecs; discovery never silently drops missing files.
export function trackedFiles(root = process.cwd(), paths = [], options = {}) {
  return execFileSync('git', ['ls-files', '--', ...paths],
    {...options, cwd: root, encoding: 'utf8', maxBuffer: Infinity}).split('\n').filter(Boolean);
}

const manifests = new Map();
export function toml(root, file) {
  const key = resolve(root, file);
  if (!manifests.has(key)) manifests.set(key, JSON.parse(execFileSync('python3', ['-I', '-c',
    "import json, sys, tomllib; json.dump(tomllib.load(open(sys.argv[1], 'rb')), sys.stdout)", key], {encoding: 'utf8'})));
  return manifests.get(key);
}

