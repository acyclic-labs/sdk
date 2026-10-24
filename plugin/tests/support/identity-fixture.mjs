import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { rename, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { promisify } from 'node:util';

// Create credential custody through the product, including the native ACL.
// Tests may then alter payload fields while retaining that same private inode.
export async function identityCredentials(binary, path, payload) {
  const server = createServer(async (request, response) => {
    for await (const _ of request) { /* drain */ }
    assert.ok(['/v1/identity/device/code', '/v1/identity/token'].includes(request.url));
    response.end(JSON.stringify(request.url.endsWith('/code')
      ? { device_code: 'fixture', user_code: 'FIXTURE', verification_uri: 'https://example.test',
        expires_in: 60, interval: 1 }
      : { access_token: 'fixture', refresh_token: 'fixture', token_type: 'Bearer', expires_in: 900 }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const created = `${path}.fixture`;
    await promisify(execFile)(binary, ['login', '--json', '--endpoint',
      `http://127.0.0.1:${server.address().port}`, '--credentials', created]);
    await rename(created, path);
    await writeFile(path, payload);
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
}
