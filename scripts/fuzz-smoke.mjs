#!/usr/bin/env node
// Thin SDK entry point to the shared campaign runner and exact-closure action cache.
import {pathToFileURL} from 'node:url';

const [binary, corpus, separator, ...args] = process.argv.slice(2);
let loaded = false;
try {
  if (!binary || !corpus || separator !== '--') throw Error('usage: node scripts/fuzz-smoke.mjs BINARY CORPUS -- LIBFUZZER_OPTIONS');
  const {campaign} = await import(pathToFileURL(process.env.SDK_FUZZ_CAMPAIGN ?? '/mnt/bb/cx/fuzz-campaign.mjs').href);
  loaded = true;
  await campaign({binary, corpus, args});
} catch (error) {
  if (!loaded) console.error('FUZZ_CAMPAIGN_TERMINAL FAIL ' + JSON.stringify({version: 1, binary, corpus, args, error: error.message}));
  process.exitCode = 1;
}
