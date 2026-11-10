import assert from 'node:assert/strict';
import {managedSession} from '../managed-session.mjs';

// Displace only the observer. Process, socket and MCP event owners retain
// their actual clocks and no event is delayed.
let sampled=false;
const clock=()=>{const now=performance.now();if(sampled)return now+60000;sampled=true;return now;};
try{
 assert.equal(process.argv.length,5,'expected ACYCLIC GRAPHCODER CODEX');
 await managedSession([...process.argv.slice(2),'lifecycle'],clock);assert.ok(sampled);
}catch(error){console.error(`step=lifecycle clock control: ${error.stack}; recovery command: node plugin/tests/hosts/proofs/session-clock.mjs ACYCLIC GRAPHCODER CODEX`);process.exitCode=1;}
