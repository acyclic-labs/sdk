import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { test } from 'node:test';

const binary = process.env.ACYCLIC_TEST_BINARY;
assert.ok(binary, 'set ACYCLIC_TEST_BINARY to the built native acyclic binary');

test('stdio identity MCP completes device login and shares session operations', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-identity-mcp-'));
  const requests = [];
  let challenge;
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    const fields = body ? JSON.parse(body) : {};
    requests.push({ path: request.url, fields, bearer: request.headers.authorization });
    let result;
    if (request.url === '/v1/identity/device/code') {
      assert.equal(request.headers['content-type'], 'application/json');
      assert.equal(fields.client_kind, 'mcp');
      assert.match(fields.code_challenge, /^[\w-]{43}$/);
      challenge = fields.code_challenge;
      result = { device_code: 'device-secret', user_code: 'ABCD',
        verification_uri: 'https://example.test/device', expires_in: 60, interval: 1 };
    } else if (request.url === '/v1/identity/token') {
      if (fields.grant_type !== 'refresh_token') assert.equal(createHash('sha256').update(fields.code_verifier).digest('base64url'), challenge);
      result = { access_token: 'access-secret', refresh_token: 'refresh-secret',
        token_type: 'Bearer', expires_in: 900 };
    } else if (request.url === '/v1/identity/session') result = { user: { id: 'user-1' } };
    else if (request.url === '/v1/identity/orgs') result = { orgs: [{ id: 'org-1' }] };
    else { response.statusCode = 404; result = { error: 'not_found' }; }
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(result));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const child = spawn(binary, ['identity-mcp', '--endpoint',
    `http://127.0.0.1:${server.address().port}`, '--credentials', join(directory, 'identity.json')],
    { stdio: ['pipe', 'pipe', 'pipe'], timeout: 20_000 });
  const closed = once(child, 'close');
  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  let id = 0;
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  async function request(method, params = {}) {
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params })}\n`);
    const line = await lines.next();
    assert.equal(line.done, false, stderr);
    const response = JSON.parse(line.value);
    assert.equal(response.id, id);
    assert.ok(!response.error, JSON.stringify(response));
    return response.result;
  }
  const call = name => request('tools/call', { name, arguments: {} });
  try {
    const initialized = await request('initialize', { protocolVersion: '2025-11-25',
      capabilities: {}, clientInfo: { name: 'fake-client', version: '1' } });
    assert.equal(initialized.serverInfo.name, 'acyclic-identity');
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' })}\n`);
    const listed = await request('tools/list');
    assert.ok(listed.tools.some(tool => tool.name === 'identity_login'));
    const login = await call('identity_login');
    assert.equal(login.structuredContent.user_code, 'ABCD');
    assert.equal(requests.length, 1, 'login start must return before polling');
    assert.equal((await call('identity_login_complete')).structuredContent.authenticated, true);
    assert.deepEqual((await call('identity_whoami')).structuredContent, { user: { id: 'user-1' } });
    assert.deepEqual((await call('identity_orgs')).structuredContent, { orgs: [{ id: 'org-1' }] });
    assert.equal((await call('identity_refresh')).structuredContent.authenticated, true);
    assert.equal(requests[2].bearer, 'Bearer access-secret');
    assert.equal(requests[4].fields.refresh_token, 'refresh-secret');
    assert.ok(!JSON.stringify(login).includes('device-secret'));
    assert.ok(!stderr.includes('secret'));
    child.stdin.end();
    const [code] = await closed;
    assert.equal(code, 0, stderr);
  } finally {
    child.kill();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});


for (const tool of ['identity_login_complete', 'identity_refresh', 'identity_whoami', 'identity_orgs'])
for (const disconnect of [false, true]) {
  test(`dispatched ${tool} exchange survives ${disconnect ? 'EOF' : 'cancellation'} before its response`,
    { timeout: 20_000 }, async () => {
      const directory = await mkdtemp(join(tmpdir(), 'acyclic-mcp-exchange-'));
      const credentials = join(directory, 'identity.json');
      let consume, release, exchanges = 0;
      const consumed = new Promise(resolve => { consume = resolve; });
      const released = new Promise(resolve => { release = resolve; });
      const server = createServer(async (request, response) => {
        let body = '';
        for await (const chunk of request) body += chunk;
        let result;
        if (request.url === '/v1/identity/device/code') {
          result = { device_code: 'consumable', user_code: 'ABCD',
            verification_uri: 'https://example.test/device', expires_in: 60, interval: 1 };
        } else if (request.url === '/v1/identity/token') {
          exchanges++;
          const fields = JSON.parse(body);
          assert.equal(fields.grant_type, tool === 'identity_login_complete'
            ? 'urn:ietf:params:oauth:grant-type:device_code' : 'refresh_token');
          if (tool !== 'identity_login_complete') assert.equal(fields.refresh_token, 'original-refresh');
          assert.equal(exchanges, 1, 'consumed device or refresh token must never be exchanged again');
          consume();
          await released;
          result = { access_token: 'issued-access', refresh_token: 'issued-refresh',
            token_type: 'Bearer', expires_in: 900 };
        } else {
          assert.ok(['/v1/identity/session', '/v1/identity/orgs'].includes(request.url));
          assert.equal(request.headers.authorization, 'Bearer issued-access');
          result = request.url.endsWith('/orgs') ? { orgs: [{ id: 'org' }] }
            : { user: { id: 'already-approved' } };
        }
        response.end(JSON.stringify(result));
      });
      await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
      const endpoint = `http://127.0.0.1:${server.address().port}`;
      if (tool !== 'identity_login_complete') await writeFile(credentials, JSON.stringify({ endpoint,
        session: { access_token: 'expired-access', refresh_token: 'original-refresh', expires_at: 0 } }));
      const child = spawn(binary, ['identity-mcp', '--endpoint', endpoint, '--credentials', credentials],
        { stdio: ['pipe', 'pipe', 'pipe'], timeout: 15_000 });
      const closed = once(child, 'close');
      const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
      const send = (id, method, params = {}) => child.stdin.write(
        `${JSON.stringify({ jsonrpc: '2.0', ...(id === null ? {} : { id }), method, params })}\n`);
      const receive = async () => {
        const line = await lines.next();
        assert.equal(line.done, false);
        return JSON.parse(line.value);
      };
      try {
        send(1, 'initialize', { protocolVersion: '2025-11-25' });
        assert.equal((await receive()).id, 1);
        if (tool === 'identity_login_complete') {
          send(2, 'tools/call', { name: 'identity_login', arguments: {} });
          assert.equal((await receive()).id, 2);
        }
        send(3, 'tools/call', { name: tool, arguments: {} });
        await consumed;
        if (disconnect) {
          child.stdin.end();
        } else {
          send(null, 'notifications/cancelled', { requestId: 3 });
          assert.equal((await receive()).error.code, -32800);
          send(4, 'ping');
          assert.deepEqual(await receive(), { jsonrpc: '2.0', id: 4, result: {} });
          send(5, 'tools/call', { name: 'identity_login_complete', arguments: {} });
          assert.equal((await receive()).error.code, -32000, 'exchange must retain exclusive ownership');
        }
        release();
        if (!disconnect) {
          let whoami;
          for (let id = 6; ; id++) {
            send(id, 'tools/call', { name: 'identity_whoami', arguments: {} });
            whoami = await receive();
            if (!whoami.error) break;
            assert.equal(whoami.error.code, -32000);
            await new Promise(resolve => setTimeout(resolve, 10));
          }
          assert.deepEqual(whoami.result.structuredContent, { user: { id: 'already-approved' } });
          child.stdin.end();
        }
        assert.equal((await closed)[0], 0);
        const saved = JSON.parse(await readFile(credentials, 'utf8'));
        assert.equal(saved.session.refresh_token, 'issued-refresh');
        assert.equal(exchanges, 1);
      } finally {
        release();
        child.kill();
        server.closeAllConnections();
        await new Promise(resolve => server.close(resolve));
        await rm(directory, { recursive: true, force: true });
      }
    });
}
