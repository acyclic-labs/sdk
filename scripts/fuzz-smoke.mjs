#!/usr/bin/env node
// Thin SDK entry point to the shared campaign runner and exact-closure action cache.
import {pathToFileURL} from 'node:url';

const [binary, corpus, separator, ...args] = process.argv.slice(2);
let terminal;
// The runner publishes through this callback; the entry point covers failures
// before publication, including import, invocation and runner cleanup errors.
const log = line => {
  const receipt = /^FUZZ_CAMPAIGN_TERMINAL (PASS|FAIL) /.exec(line);
  if (receipt) {
    if (terminal) throw Error("campaign published more than one terminal receipt");
    terminal = receipt[1];
  }
  console.log(line);
};
try {
  if (!binary || !corpus || separator !== '--') throw Error('usage: node scripts/fuzz-smoke.mjs BINARY CORPUS -- LIBFUZZER_OPTIONS');
  if (!process.env.SDK_FUZZ_CAMPAIGN) throw Error('SDK_FUZZ_CAMPAIGN must name the campaign runner module');
  const {campaign} = await import(pathToFileURL(process.env.SDK_FUZZ_CAMPAIGN).href);
  if (typeof campaign !== 'function') throw Error('campaign export must be a function');
  await campaign({binary, corpus, args, log});
  if (!terminal) throw Error('campaign returned without a terminal receipt');
  if (terminal === 'FAIL') process.exitCode = 1;
} catch (error) {
  if (!terminal) console.error('FUZZ_CAMPAIGN_TERMINAL FAIL ' + JSON.stringify({version: 1, status: 'FAIL', binary, corpus, args, error: error instanceof Error ? error.message : String(error)}));
  process.exitCode = 1;
}
