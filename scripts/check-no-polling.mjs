import {execFileSync} from 'node:child_process';
import {readFileSync, writeFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {gitBatchSources} from './source-policy/git.mjs';

export async function checkNoPolling(args, {cwd = process.cwd(), env = process.env, logger = console} = {}) {
 const [base, output] = args;
 const inventory = base === '--inventory';
 const recovery = 'node scripts/check-no-polling.mjs ' + (inventory
  ? '--inventory ' + JSON.stringify(output ?? 'no-polling-census.json')
  : JSON.stringify(base ?? 'HEAD'));
 try {
  const {sourceInScope} = await import('./no-polling-scope.mjs');
  const {newProjectPollingSites, projectPollingSites} = await import('./source-policy/project.mjs');
  if (inventory ? args.length !== 2 || !output || output.startsWith('-')
   : args.length !== 1 || !base || base.startsWith('-'))
   throw new Error('expected one baseline Git revision, or --inventory followed by a new output file');
  const git = (args, options) => execFileSync('git', args, {cwd, env, stdio: ['pipe', 'pipe', 'pipe'], maxBuffer: Infinity, ...options});
  const files = git(['ls-files', '--cached', '--others', '--exclude-standard', '-z'], {encoding: 'utf8'})
   .split('\0').filter(file => file && sourceInScope(file));
  const afterSources = new Map();
  for (const file of files) {
   try { afterSources.set(file, readFileSync(resolve(cwd, file), 'utf8')); }
   catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  if (inventory) {
   const revision = git(['rev-parse', '--verify', 'HEAD'], {encoding: 'utf8'}).trim();
   const sites = projectPollingSites(afterSources);
   writeFileSync(resolve(cwd, output), JSON.stringify({version: 1, revision, files: afterSources.size, sites}, null, 2) + '\n', {flag: 'wx'});
   logger.log(`Polling candidates: source files=${afterSources.size} sites=${sites.length} inventory=${output}`);
  } else {
   const revision = git(['rev-parse', '--verify', base + '^{commit}'], {encoding: 'utf8'}).trim();
   const baselineFiles = git(['ls-tree', '-r', '--name-only', '-z', revision], {encoding: 'utf8'})
    .split('\0').filter(file => file && sourceInScope(file));
   const unique = [...new Set([...files, ...baselineFiles])].sort();
   if (unique.some(file => /[\r\n]/.test(file))) throw new Error('source path contains a Git batch delimiter');
   const input = unique.map(file => revision + ':' + file + '\n').join('');
   const original = git(['cat-file', '--batch'], {input});
   const dispositions = JSON.parse(readFileSync(resolve(cwd, 'scripts/no-polling-dispositions.json'), 'utf8'));
   const beforeSources = gitBatchSources(unique, original);
   const refused = newProjectPollingSites(beforeSources, afterSources, dispositions, {inScope: sourceInScope});
   if (refused.length) {
    for (const site of refused) logger.error(`step=no-polling file=${site.file}:${site.line} kind=${site.kind} fingerprint=${site.fingerprint}`);
    throw new Error('remove each new wait/retry, or record its exact producer, terminal and negative control under the repository rule');
   }
   logger.log('No new unclassified polling sites');
  }
  return 0;
 } catch (error) {
  const command = error.code === 'ERR_MODULE_NOT_FOUND' ? 'bun install --frozen-lockfile'
   : error.code === 'EEXIST' && inventory ? 'node scripts/check-no-polling.mjs --inventory ' + JSON.stringify(output + '.next') : recovery;
  logger.error(`step=no-polling ${error.message}; recovery command: ${command}`);
  return 1;
 }
}

if (import.meta.main) process.exitCode = await checkNoPolling(process.argv.slice(2));
