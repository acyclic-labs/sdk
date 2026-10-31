import {execFileSync} from 'node:child_process';
import {existsSync, readFileSync} from 'node:fs';

const base = process.argv[2];
const recovery = 'node scripts/check-no-polling.mjs ' + JSON.stringify(base ?? 'HEAD');
try {
 const {sourceInScope} = await import('./no-polling.mjs');
 const {newProjectPollingSites} = await import('./no-polling-project.mjs');
 if (process.argv.length !== 3 || !base || base.startsWith('-')) throw new Error('expected one baseline Git revision');
 const revision = execFileSync('git', ['rev-parse', '--verify', base + '^{commit}'], {encoding: 'utf8', maxBuffer: Infinity}).trim();
 const files = execFileSync('git', ['ls-files', '--cached', '--others', '--exclude-standard', '-z'], {encoding: 'utf8', maxBuffer: Infinity})
  .split('\0').filter(file => file && sourceInScope(file));
 const baselineFiles = execFileSync('git', ['ls-tree', '-r', '--name-only', '-z', revision], {encoding: 'utf8', maxBuffer: Infinity})
  .split('\0').filter(file => file && sourceInScope(file));
 const unique = [...new Set([...files, ...baselineFiles])].sort();
 if (unique.some(file => /[\r\n]/.test(file))) throw new Error('source path contains a Git batch delimiter');
 const input = unique.map(file => revision + ':' + file + '\n').join('');
 const original = execFileSync('git', ['cat-file', '--batch'], {input, maxBuffer: Infinity});
 const dispositions = JSON.parse(readFileSync('scripts/no-polling-dispositions.json', 'utf8'));
 let at = 0;
 const beforeSources = new Map();
 const afterSources = new Map();
 for (const file of unique) {
  const headerEnd = original.indexOf(10, at);
  if (headerEnd < 0) throw new Error('truncated Git object header for ' + file);
  const header = original.subarray(at, headerEnd).toString('utf8');
  at = headerEnd + 1;
  if (!header.endsWith(' missing')) {
   const fields = header.split(' ');
   const size = Number(fields[2]);
   if (fields[1] !== 'blob' || !Number.isSafeInteger(size) || size < 0 || at + size >= original.length) throw new Error('invalid Git object for ' + file);
   const before = original.subarray(at, at + size);
   at += size + 1;
   beforeSources.set(file, before.toString('utf8'));
  }
  if (existsSync(file)) afterSources.set(file, readFileSync(file, 'utf8'));
 }
 const refused = newProjectPollingSites(beforeSources, afterSources, dispositions);
 if (refused.length) {
  for (const site of refused) console.error(`step=no-polling file=${site.file}:${site.line} kind=${site.kind} fingerprint=${site.fingerprint}`);
  throw new Error('remove each new wait/retry, or record its exact producer, terminal and negative control under the repository rule');
 }
 console.log('No new unclassified polling sites');
} catch (error) {
 console.error(`step=no-polling ${error.message}; recovery command: ${error.code === 'ERR_MODULE_NOT_FOUND' ? 'bun install --frozen-lockfile' : recovery}`);
 process.exitCode = 1;
}
