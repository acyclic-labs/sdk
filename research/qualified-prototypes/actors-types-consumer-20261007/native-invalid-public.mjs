import { readFile } from 'node:fs/promises';
import { ActorsClient } from '@acyclic-labs/actors';

const caCertificate = await readFile(new URL('./native-test-cert.pem', import.meta.url));
const client = new ActorsClient({
  endpoint: 'https://127.0.0.1:54443',
  token: 'fixture-token',
  caCertificate,
});

const cases = [
  ['empty-actor-id', () => client.inspectActor({ actorId: '' })],
  ['zero-hash-and-limits', () => client.createActor({
    codeSha256: new Uint8Array(0),
    homeRegion: 'eu',
    bindings: [],
    limits: { handlerTimeoutMillis: 0n, memoryBytes: 0n, checkpointBytes: 0n },
    subscriptions: [],
    idempotencyKey: 'invalid',
  })],
];

for (const [name, run] of cases) {
  try {
    await run();
    console.log(JSON.stringify({ name, result: 'resolved' }));
  } catch (error) {
    console.log(JSON.stringify({ name, result: 'rejected', name: error?.name, code: error?.code, message: error?.message }));
  }
}
