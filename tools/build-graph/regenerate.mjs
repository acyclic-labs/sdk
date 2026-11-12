import {spawnSync} from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const configuration = process.argv[2] ?? process.env.BAZEL_GENERATION_CONFIG;
if (!configuration) throw Error('usage: node tools/build-graph/regenerate.mjs CONFIG_JSON (or set BAZEL_GENERATION_CONFIG)');
const root = fileURLToPath(new URL('../../', import.meta.url));
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'build-graph-metadata-'));
try {
  const result = spawnSync('cargo', ['metadata', '--locked', '--format-version', '1'], {cwd: root, encoding: 'utf8', maxBuffer: 128 * 1024 * 1024});
  if (result.error) throw result.error;
  if (result.status !== 0) { process.stderr.write(result.stderr); process.exitCode = result.status ?? 1; }
  else {
    const metadata = path.join(dir, 'metadata.json');
    fs.writeFileSync(metadata, result.stdout);
    const generated = spawnSync(process.execPath, [fileURLToPath(new URL('./generate.mjs', import.meta.url)), metadata, root, path.resolve(configuration)], {stdio: 'inherit'});
    process.exitCode = generated.status ?? 1;
  }
} finally { fs.rmSync(dir, {recursive: true, force: true}); }
