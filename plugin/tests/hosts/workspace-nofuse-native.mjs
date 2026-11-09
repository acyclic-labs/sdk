#!/usr/bin/env node
// Only the external host is a fixture; admission, launch and capture use the
// shipping client and owner, without credentials or a model provider.
import assert from 'node:assert/strict';
import {writeFileSync} from 'node:fs';
if(process.env.FIXTURE_HEADLESS==='1')assert.equal(process.argv.at(-1),'no-FUSE root prompt');
assert.ok(process.env.ACYCLIC_TASK);
assert.equal(process.env.ACYCLIC_WORKSPACE,process.cwd());
writeFileSync('native-root.txt',process.env.ACYCLIC_TASK);
console.log(JSON.stringify({type:'native.started',task:process.env.ACYCLIC_TASK}));
