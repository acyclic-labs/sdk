import { ActorsClient } from '@acyclic-labs/actors';
console.log('platform=' + process.platform);
const c = new ActorsClient({endpoint:'https://127.0.0.1:54443', token:'fixture-token', caCertificate: new TextEncoder().encode('fixture')});
console.log('transport=' + await c.transport);
const names=['addSubscription','checkpointActor','createActor','inspectActor','invokeActor','removeSubscription','resumeSubscription','updateActor'];
for (const n of names) { try { await c[n]({}); console.log(n+':ok'); } catch (e) { console.log(n+':'+e.message); } }