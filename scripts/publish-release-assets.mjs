import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Annotated tags retain an already-existing immutable object, so each step
// advances through the actual ancestry rather than rechecking visibility.
export function resolveTagTarget(ref, readTag) {
  while (ref?.type === 'tag') ref = readTag(ref.sha);
  return ref;
}

export function admitRelease(release, source, version) {
  if (release.tag_name !== `acyclic-v${version}` || release.target_commitish !== source) {
    throw new Error('Existing release names a different version or source; refusing to replace it');
  }
}

export function admitAsset(name, expected, actual) {
  if (expected !== actual) throw new Error(`${name} already exists with different contents`);
}

export function assets(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory() ? assets(file) : entry.isFile() ? [file] : [];
  }).sort();
}

async function digest(file) {
  const hash = createHash('sha256');
  for await (const bytes of fs.createReadStream(file)) hash.update(bytes);
  return `sha256:${hash.digest('hex')}`;
}

export async function publish({ env = process.env, directory = env.RELEASE_ASSET_DIR, gh = (...args) => execFileSync('gh', args, { encoding: 'utf8', maxBuffer: Infinity }) } = {}) {
  if (!directory) throw new Error('An explicit qualified release asset directory is required');
  const files = assets(directory);
  if (!files.length) throw new Error('Qualified release asset directory is empty');
  const { SOURCE_SHA: source, VERSION: version, GITHUB_REPOSITORY: repository } = env;
  if (!/^[0-9a-f]{40}$/.test(source || '') || !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version || '')) {
    throw new Error('Exact release source and semver are required');
  }
  const tag = `acyclic-v${version}`;
  // An existing tag must resolve to the qualified source, including annotated tags.
  let ref;
  try {
    ref = JSON.parse(gh('api', `repos/${repository}/git/ref/tags/${tag}`)).object;
  } catch (error) {
    if (!String(error.stderr).includes('HTTP 404')) throw error;
  }
  if (ref?.type === 'tag') {
    ref = resolveTagTarget(ref, sha => JSON.parse(gh('api', `repos/${repository}/git/tags/${sha}`)).object);
  }
  if (ref && (ref.type !== 'commit' || ref.sha !== source)) {
    throw new Error('Existing tag points to a different source');
  }
  let release;
  try {
    release = JSON.parse(gh('api', `repos/${repository}/releases/tags/${tag}`));
  } catch (error) {
    if (!String(error.stderr).includes('HTTP 404')) throw error;
    gh('release', 'create', tag, '--repo', repository, '--target', source, '--title', `Acyclic ${version}`,
      '--notes', `Qualified SDK release from ${source}`, '--draft');
    release = JSON.parse(gh('api', `repos/${repository}/releases/tags/${tag}`));
  }
  admitRelease(release, source, version);
  const byName = new Map();
  for (const asset of release.assets) byName.set(asset.name, asset);
  const names = new Set();
  for (const file of files) {
    const name = path.basename(file);
    if (names.has(name)) throw new Error(`Duplicate release asset name ${name}`);
    names.add(name);
    const expected = await digest(file);
    const existing = byName.get(name);
    if (existing) {
      let actual = existing.digest;
      if (!actual) {
        const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-release-'));
        try {
          gh('release', 'download', tag, '--repo', repository, '--pattern', name, '--dir', temp);
          actual = await digest(path.join(temp, name));
        } finally {
          const relative = path.relative(path.resolve(os.tmpdir()), path.resolve(temp));
          if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
            throw new Error('Refusing to remove a temporary directory outside the temporary root');
          }
          fs.rmSync(temp, { recursive: true });
        }
      }
      admitAsset(name, expected, actual);
      continue;
    }
    gh('release', 'upload', tag, file, '--repo', repository);
  }
  gh('release', 'edit', tag, '--repo', repository, '--draft=false');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await publish();
