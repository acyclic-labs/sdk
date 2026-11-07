import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, chmodSync, openSync, readSync, closeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { sha256File } from './file-digest.mjs';
import { checkReleaseVersion } from './release-source.mjs';
import { buildManifest } from './verify-products.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const recovery = 'node scripts/package-products.mjs OUTPUT X64_DIST ARM64_DIST';
const run = (command, args, cwd = root) => execFileSync(command, args, { cwd, encoding: 'utf8' }).trim();

function admitExecutable(file, machine) {
  const header = Buffer.alloc(20);
  const descriptor = openSync(file, 'r');
  try {
    if (readSync(descriptor, header, 0, header.length, 0) !== header.length
        || !header.subarray(0, 6).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1]))
        || header.readUInt16LE(18) !== machine) {
      throw new Error(`ELF architecture admission failed for ${file}`);
    }
  } finally { closeSync(descriptor); }
}

async function main() {
  const [output, x64, arm64] = process.argv.slice(2).map(value => resolve(value));
  if (!output || !x64 || !arm64 || process.argv.length !== 5) throw new Error('three explicit directories are required');
  if (run('git', ['status', '--porcelain', '--untracked-files=normal'])) throw new Error('committed release source is required');
  const plugin = JSON.parse(readFileSync(join(root, 'plugin/package.json'), 'utf8'));
  const version = checkReleaseVersion(plugin.version);
  const pythonVersion = /^version = "([^"]+)"$/m.exec(readFileSync(join(root, 'scripts/benchmarks/pyproject.toml'), 'utf8'))?.[1];
  if (pythonVersion !== version) throw new Error('Harbor adapter version differs from product version');
  const source = run('git', ['rev-parse', 'HEAD']);
  const epoch = run('git', ['show', '-s', '--format=%ct', 'HEAD']);
  mkdirSync(output, { recursive: true });
  if (readdirSync(output).length) throw new Error('product output directory must be empty');
  const temporary = mkdtempSync(join(tmpdir(), 'acyclic-products-'));
  const files = run('git', ['ls-files', 'plugin']).split('\n').filter(file => {
    const local = file.slice('plugin/'.length);
    return local === 'package.json' || plugin.files.some(entry => local === entry || local.startsWith(`${entry}/`));
  });
  const names = [];
  try {
    for (const [platform, directory, machine] of [['linux-x64', x64, 62], ['linux-arm64', arm64, 183]]) {
      const build = buildManifest(readFileSync(join(directory, 'PRODUCT-BUILD.json'), 'utf8'), source);
      const stage = join(temporary, platform);
      mkdirSync(stage);
      for (const file of files) {
        const target = join(stage, 'plugin', file.slice('plugin/'.length));
        mkdirSync(dirname(target), { recursive: true });
        copyFileSync(join(root, file), target);
      }
      copyFileSync(join(directory, 'acyclic'), join(stage, 'plugin/bin/acyclic'));
      copyFileSync(join(directory, 'graphcoder'), join(stage, 'graphcoder'));
      for (const [name, entry] of [['acyclic', 'plugin/bin/acyclic'], ['graphcoder', 'graphcoder']]) {
        // Admit the private snapshot that tar consumes, after the sole copy.
        const snapshot = join(stage, entry);
        admitExecutable(snapshot, machine);
        if (build.hashes[name] !== await sha256File(snapshot)) throw new Error(`${platform}/${name} differs from its original build`);
      }
      for (const executable of ['plugin/bin/acyclic', 'graphcoder']) chmodSync(join(stage, executable), 0o755);
      for (const [kind, entry] of [['graphcoder', 'graphcoder'], ['acyclic-plugin', 'plugin']]) {
        const name = `${kind}-${version}-${platform}.tar.gz`;
        run('tar', ['--sort=name', `--mtime=@${epoch}`, '--owner=0', '--group=0', '--numeric-owner',
          '--use-compress-program=gzip -n', '-cf', join(output, name), entry], stage);
        names.push(name);
      }
    }
    writeFileSync(join(output, 'PRODUCTS.json'), JSON.stringify({ format: 1, version, source, assets: names }, null, 2) + '\n', { flag: 'wx' });
    names.push('PRODUCTS.json');
    const checksums = await Promise.all(names.sort().map(async name => `${await sha256File(join(output, name))}  ${name}\n`));
    writeFileSync(join(output, 'SHA256SUMS'), checksums.join(''), { flag: 'wx' });
  } finally { rmSync(temporary, { recursive: true }); }
}

try { await main(); }
catch (error) { throw new Error(`product packaging step failed: ${error.message}; recovery: ${recovery}`, { cause: error }); }
