#!/usr/bin/env node
// The shipping launcher and owner are real binaries. This process only marks
// entry into the external host, without requiring credentials or a model.
import assert from 'node:assert/strict';
import {writeFileSync} from 'node:fs';
assert.equal(process.argv.at(-1),'no-FUSE root prompt');
assert.ok(process.env.ACYCLIC_TASK);
assert.equal(process.env.ACYCLIC_WORKSPACE,process.cwd());
writeFileSync('native-root.txt',process.env.ACYCLIC_TASK);
console.log(JSON.stringify({type:'native.started',task:process.env.ACYCLIC_TASK}));
