import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { verifyNativeImage } from './verify-release-binary.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

// The plugin and GraphCoder share one supported platform inventory and parser.
export function assemble({ binaries, output, universal = false }) {
  const targets = JSON.parse(readFileSync(join(root, 'plugin/bin/targets.json'), 'utf8')).targets;
  const version = JSON.parse(readFileSync(join(root, 'plugin/package.json'), 'utf8')).version;
  const entries = new Map();
  for (const value of binaries) {
    const index = value.indexOf('=');
    const target = value.slice(0, index), source = value.slice(index + 1);
    if (index < 1 || !Object.hasOwn(targets, target) || entries.has(target)) {
      throw new Error(`Invalid or duplicate GraphCoder target: ${value}`);
    }
    verifyNativeImage(readFileSync(source), target);
    entries.set(target, source);
  }
  if (!entries.size || (universal && Object.keys(targets).some(target => !entries.has(target)))) {
    throw new Error('Missing GraphCoder release binaries');
  }
  // Admit the entire inventory before publishing any bytes.
  mkdirSync(output, { recursive: true });
  const names = [];
  for (const [target, source] of entries) {
    const name = `graphcoder-${target}${target.startsWith('win32-') ? '.exe' : ''}`;
    copyFileSync(source, join(output, name));
    names.push(name);
  }
  for (const name of ['install-graphcoder.sh', 'install-graphcoder.ps1']) {
    const template = readFileSync(join(root, 'scripts', name), 'utf8');
    writeFileSync(join(output, name), template.replace('@VERSION@', version));
    names.push(name);
  }
  return names;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const binaries = [];
  let output, universal = false;
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--require-universal') universal = true;
    else if (args[i] === '--out' && args[i + 1]) output = resolve(args[++i]);
    else if (args[i] === '--binary' && args[i + 1]) binaries.push(args[++i]);
    else throw new Error(`Unknown or incomplete argument: ${args[i]}`);
  }
  if (!output) throw new Error('Required: --out DIRECTORY --binary TARGET=EXECUTABLE [--require-universal]');
  assemble({ binaries, output, universal });
}
