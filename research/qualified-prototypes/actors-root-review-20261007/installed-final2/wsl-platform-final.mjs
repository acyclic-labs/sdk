import { ActorsClient } from '@acyclic-labs/actors';
console.log('platform=' + process.platform);
const c = new ActorsClient({endpoint:'http://127.0.0.1:1', token:'fixture-token'});
try { console.log('transport=' + await c.transport); }
catch (e) { console.log('error=' + e.message); console.log('cause=' + (e.cause?.message ?? '')); }