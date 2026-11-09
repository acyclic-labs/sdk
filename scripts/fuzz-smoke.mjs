#!/usr/bin/env node
// Thin SDK entry point to the shared campaign runner and exact-closure action cache.
import {pathToFileURL} from 'node:url';

const [binary, corpus, separator, ...args] = process.argv.slice(2);
let terminal = false;
// The runner publishes through this callback; the entry point covers failures
// before publication, including import, invocation and runner cleanup errors.
const log = line => {
  if (/^FUZZ_CAMPAIGN_TERMINAL (PASS|FAIL) /.test(line)) terminal = true;
  console.log(line);
};
try {
  if (!binary || !corpus || separator !== '--') throw Error('usage: node scripts/fuzz-smoke.mjs BINARY CORPUS -- LIBFUZZER_OPTIONS');
  const {campaign} = await import(pathToFileURL(process.env.SDK_FUZZ_CAMPAIGN ?? '/mnt/bb/cx/fuzz-campaign.mjs').href);
  if (typeof campaign !== 'function') throw Error('campaign export must be a function');
  await campaign({binary, corpus, args, log});
  if (!terminal) throw Error('campaign returned without a terminal receipt');
} catch (error) {
  if (!terminal) console.error('FUZZ_CAMPAIGN_TERMINAL FAIL ' + JSON.stringify({version: 1, status: 'FAIL', binary, corpus, args, error: error instanceof Error ? error.message : String(error)}));
  process.exitCode = 1;
}
