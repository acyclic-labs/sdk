import assert from 'node:assert/strict';
import net from 'node:net';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

const require = createRequire(import.meta.url);
assert.ok(process.argv[2], 'pass the absolute extracted binding.cjs path');
const binding = require(process.argv[2]);
const binaries = Object.keys(require.cache).filter(path => path.endsWith('.node')).map(path => ({
  path, sha256: createHash('sha256').update(readFileSync(path)).digest('hex'),
}));
assert.equal(binaries.length, 1, 'probe must load exactly one native addon');
if (process.argv[3]) assert.equal(binaries[0].sha256, process.argv[3].toLowerCase(), 'loaded addon differs from the package artifact');
const errors = [];
process.on('unhandledRejection', error => { errors.push(String(error)); console.error(String(error)); process.exitCode = 1; });
process.on('uncaughtException', error => { errors.push(String(error)); console.error(String(error)); process.exitCode = 1; });
console.log(JSON.stringify({ phase: 'loaded', binaries }));
const sockets = new Set();
const server = net.createServer(socket => {
  sockets.add(socket);
  socket.on('close', () => sockets.delete(socket));
  // Drain the ClientHello so Node can observe EOF; never respond to TLS.
  socket.resume();
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
try {
  const cancellation = new binding.NativeStreamCancellation();
  const pending = binding.NativeStreamClient.connectResult(
    `https://127.0.0.1:${server.address().port}`, 'stalled', cancellation,
  );
  let earlyResult;
  pending.then(value => { earlyResult = value; });
  const acceptedDeadline = Date.now() + 5000;
  while (sockets.size === 0 && Date.now() < acceptedDeadline) await new Promise(resolve => setTimeout(resolve, 10));
  console.log(JSON.stringify({ phase: 'accepted', activeSocketCount: sockets.size, earlyResult }));
  assert.equal(sockets.size, 1, 'direct native connect never reached TLS fixture');
  cancellation.cancel();
  let timeout;
  const result = await Promise.race([
    pending,
    new Promise((_, reject) => { timeout = setTimeout(() => reject(new Error('native cancellation did not settle')), 5000); }),
  ]).finally(() => clearTimeout(timeout));
  const closeDeadline = Date.now() + 5000;
  while (sockets.size > 0 && Date.now() < closeDeadline) await new Promise(resolve => setTimeout(resolve, 10));
  console.log(JSON.stringify({ binaries, cancellationRequested: cancellation.cancelled, result,
    activeSocketCount: sockets.size, unhandledErrors: errors }));
  assert.equal(result.error?.code, 'cancelled');
  assert.equal(sockets.size, 0, 'direct native cancellation retains the TLS socket');
  assert.equal(errors.length, 0);
  assert.equal(createHash('sha256').update(readFileSync(binaries[0].path)).digest('hex'), binaries[0].sha256,
    'native artifact changed during qualification');
} finally {
  for (const socket of sockets) socket.destroy();
  await new Promise(resolve => server.close(resolve));
}
