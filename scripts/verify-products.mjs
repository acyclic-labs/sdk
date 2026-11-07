import { readFileSync, readdirSync, lstatSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { sha256File } from './file-digest.mjs';
import { checkReleaseVersion } from './release-source.mjs';

export function productManifest(bytes, version, source) {
  checkReleaseVersion(version);
  if (!/^[0-9a-f]{40}$/.test(source ?? '')) throw new Error('exact product source is required');
  const expected = ['graphcoder', 'acyclic-plugin'].flatMap(product =>
    ['linux-x64', 'linux-arm64'].map(platform => `${product}-${version}-${platform}.tar.gz`)).concat('PRODUCTS.json').sort();
  const manifest = JSON.parse(bytes);
  if (manifest.format !== 1 || manifest.source !== source || manifest.version !== version
      || JSON.stringify([...manifest.assets, 'PRODUCTS.json'].sort()) !== JSON.stringify(expected)) {
    throw new Error('product identity or inventory differs from the selected release');
  }
  return expected;
}

export function buildManifest(bytes, source) {
  const build = JSON.parse(bytes);
  if (build.format !== 1 || build.source !== source || build.dirty !== false
      || !['graphcoder', 'acyclic'].every(name => /^[0-9a-f]{64}$/.test(build.hashes?.[name] ?? ''))) {
    throw new Error('build identity differs from the committed release source');
  }
  return build;
}

export async function verifyProducts(directory, version, source) {
  const expected = productManifest(readFileSync(join(directory, 'PRODUCTS.json'), 'utf8'), version, source);
  const actual = readdirSync(directory).sort();
  if (JSON.stringify(actual) !== JSON.stringify([...expected, 'SHA256SUMS'].sort())) throw new Error('unexpected product release files');
  if (!actual.every(name => lstatSync(join(directory, name)).isFile())) throw new Error('product assets must be regular files');
  const lines = await Promise.all(expected.map(async name => `${await sha256File(join(directory, name))}  ${name}\n`));
  if (readFileSync(join(directory, 'SHA256SUMS'), 'utf8') !== lines.join('')) throw new Error('product release checksum mismatch');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await verifyProducts(process.argv[2], process.env.VERSION, process.env.SOURCE_SHA); }
  catch (error) { throw new Error(`prebuilt product verification failed: ${error.message}; recovery: VERSION=VERSION SOURCE_SHA=COMMIT node scripts/verify-products.mjs DIRECTORY`, { cause: error }); }
}
