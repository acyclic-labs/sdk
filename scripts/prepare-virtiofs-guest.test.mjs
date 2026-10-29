import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

test('guest downloads preserve complete metadata and artifact digests across transport retry', { timeout: 10_000 }, async t => {
  const metadata = Buffer.from(JSON.stringify({ padding: 'x'.repeat(2 * 1024 * 1024 + 1) }));
  const chunk = Buffer.alloc(64 * 1024, 0xa5);
  const chunks = 1025;
  const checksum = createHash('sha256');
  for (let index = 0; index < chunks; index++) checksum.update(chunk);
  const expected = checksum.digest('hex');
  let artifacts = 0;
  const server = createServer((request, response) => {
    response.setHeader('Connection', 'close');
    if (request.url === '/metadata') {
      response.setHeader('Content-Length', metadata.length);
      response.end(metadata);
      return;
    }
    artifacts++;
    response.setHeader('Content-Length', chunks * chunk.length);
    if (artifacts === 1) {
      // An actual truncated response must retry from byte zero, not retain a prefix.
      response.end(chunk);
      return;
    }
    pipeline(Readable.from((function* () {
      for (let index = 0; index < chunks; index++) yield chunk;
    })()), response).catch(error => response.destroy(error));
  });
  t.after(() => { server.closeAllConnections(); server.close(); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;
  const source = fileURLToPath(new URL('./prepare-virtiofs-guest.py', import.meta.url));
  const child = spawn('python3', ['-c', `
import hashlib, json, runpy, sys, tempfile
fetch = runpy.run_path(sys.argv[1])["fetch"]
with tempfile.TemporaryFile() as output:
    metadata_digest = fetch(sys.argv[2] + "/metadata", output)
    output.seek(0)
    padding = len(json.load(output)["padding"])
    artifact_digest = fetch(sys.argv[2] + "/artifact", output)
    length = output.tell()
    output.seek(0)
    reread = hashlib.file_digest(output, "sha256").hexdigest()
print(json.dumps([metadata_digest, padding, artifact_digest, length, reread]))
`, source, base], { signal: t.signal, stdio: ['ignore', 'pipe', 'pipe'] });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  let stdout = '', stderr = '';
  child.stdout.on('data', data => { stdout += data; });
  child.stderr.on('data', data => { stderr += data; });
  const [status] = await once(child, 'close');
  assert.equal(status, 0, stderr);
  assert.deepEqual(JSON.parse(stdout), [
    createHash('sha256').update(metadata).digest('hex'), 2 * 1024 * 1024 + 1,
    expected, chunks * chunk.length, expected,
  ]);
  assert.equal(artifacts, 2, 'one transport retry; no readiness probes');
});
