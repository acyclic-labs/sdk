import { commandOutput } from "./process-capture.mjs";
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export function admitRelease(release, source, version) {
  if (release.tag_name !== `acyclic-v${version}` || release.target_commitish !== source) {
    throw new Error('Existing release names a different version or source; refusing to replace it');
  }
}

export function admitAsset(name, expected, actual) {
  if (expected !== actual) throw new Error(`${name} already exists with different contents`);
}

export async function* assets(directory) {
  const directories = [];
  try {
    directories.push(await fs.promises.opendir(directory));
    while (directories.length) {
      const current = directories.at(-1);
      const entry = await current.read();
      if (!entry) {
        directories.pop();
        await current.close();
        continue;
      }
      const file = path.join(current.path, entry.name);
      if (entry.isDirectory()) directories.push(await fs.promises.opendir(file));
      else if (entry.isFile()) yield file;
    }
  } finally {
    const closed = await Promise.allSettled(directories.map(directory => directory.close()));
    const failed = closed.find(result => result.status === 'rejected');
    if (failed) throw failed.reason;
  }
}

async function digest(file) {
  const hash = createHash('sha256');
  for await (const bytes of fs.createReadStream(file)) hash.update(bytes);
  return `sha256:${hash.digest('hex')}`;
}

export async function publish({ env = process.env, directory = env.RELEASE_ASSET_DIR, gh = (...args) => commandOutput('gh', args, { encoding: 'utf8' }) } = {}) {
  if (!directory) throw new Error('An explicit qualified release asset directory is required');
  const files = assets(directory);
  try {
    const first = await files.next();
    if (first.done) throw new Error('Qualified release asset directory is empty');
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
    const tags = new Set();
    while (ref?.type === 'tag') {
      if (tags.has(ref.sha)) throw new Error('Release tag references form a cycle');
      tags.add(ref.sha);
      ref = JSON.parse(gh('api', `repos/${repository}/git/tags/${ref.sha}`)).object;
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
    // One index serves remote lookup and local duplicate admission. Each remote
    // name is examined once, including when every local asset resumes late in
    // a large release. A null value marks an already visited local name.
    const names = new Map();
    for (const asset of release.assets) {
      const name = asset.name;
      if (names.has(name)) throw new Error(`Duplicate published asset name ${name}`);
      names.set(name, asset);
    }
    delete release.assets;
    for (let item = first; !item.done; item = await files.next()) {
      const file = item.value;
      const name = path.basename(file);
      const existing = names.get(name);
      if (existing === null) throw new Error(`Duplicate release asset name ${name}`);
      names.set(name, null);
      const expected = await digest(file);
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
  } finally {
    await files.return();
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await publish();
