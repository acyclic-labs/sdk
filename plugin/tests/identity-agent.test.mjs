import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, createPublicKey, randomBytes, verify } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { test } from 'node:test';

const binary = process.env.ACYCLIC_TEST_BINARY;
assert.ok(binary, 'set ACYCLIC_TEST_BINARY to the built native acyclic binary');

function run(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, { stdio: ['ignore', 'pipe', 'pipe'], timeout: 20_000 });
    let stdout = '', stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', code => resolve({ code, stdout, stderr }));
  });
}

test('CLI and MCP agent signup solve the published proof, preserve keys and sign renewal/upgrade', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-agent-'));
  const challenge = 'c'.repeat(43);
  const registration = { user_id: 'user-1', org_id: 'org-1', key_id: 'key-1',
    account_id: 'account-1', tier: 'agent', certificate: 'cert-1', birth: 'birth-1',
    expires_at: Math.floor(Date.now() / 1000) + 2_592_000 };
  let publicKey, pkce, malformedRenewal, failSignup = false, mismatchRenewal = false, upgraded = false;
  const requests = [];
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    const fields = body ? JSON.parse(body) : {};
    requests.push(request.url);
    let result;
    if (request.url === '/v1/identity/signup/challenge') {
      assert.equal(request.method, 'GET');
      result = { challenge, difficulty: 10, expires_at: Math.floor(Date.now() / 1000) + 600 };
    } else if (request.url === '/v1/identity/signup') {
      assert.equal(request.headers['content-type'], 'application/json');
      assert.equal(fields.challenge, challenge);
      assert.match(fields.public_key, /^[\w-]{43}$/);
      assert.match(fields.nonce, /^[\w-]{1,128}$/);
      const hash = createHash('sha256').update(`${challenge}.${fields.public_key}.${fields.nonce}`).digest();
      assert.equal(hash[0], 0);
      assert.equal(hash[1] & 0xc0, 0);
      publicKey = createPublicKey({ format: 'jwk', key: { kty: 'OKP', crv: 'Ed25519', x: fields.public_key } });
      assert.ok(verify(null, Buffer.from(`acyclic-agent-signup-v1\n${fields.public_key}\n${fields.challenge}\n${fields.nonce}`),
        publicKey, Buffer.from(fields.signature, 'base64url')));
      response.statusCode = failSignup ? 503 : 201;
      result = failSignup ? { error: 'unavailable' } : registration;
    } else if (request.url === '/v1/identity/signup/renew') {
      assert.equal(fields.challenge, challenge);
      assert.equal(fields.org_id, 'org-1');
      assert.equal(fields.key_id, 'key-1');
      assert.ok(verify(null, Buffer.from(`acyclic-agent-renew-v1\norg-1\nkey-1\n${challenge}`),
        publicKey, Buffer.from(fields.signature, 'base64url')));
      result = { org_id: mismatchRenewal ? 'wrong-org' : 'org-1', key_id: 'key-1',
        certificate: 'cert-2', birth: 'birth-1', expires_at: registration.expires_at + 100 };
      if (malformedRenewal) result = { ...result, ...malformedRenewal };
    } else if (request.url === '/v1/identity/device/code') {
      assert.equal(fields.client_kind, 'cli');
      pkce = fields.code_challenge;
      if (upgraded) {
        assert.equal(fields.agent_org_id, undefined);
        assert.equal(fields.agent_key_id, undefined);
        assert.equal(fields.agent_signature, undefined);
      } else {
        assert.equal(fields.agent_org_id, 'org-1');
        assert.equal(fields.agent_key_id, 'key-1');
        assert.ok(verify(null, Buffer.from(`acyclic-agent-upgrade-v1\norg-1\nkey-1\n${pkce}`),
          publicKey, Buffer.from(fields.agent_signature, 'base64url')));
      }
      result = { device_code: 'device-secret', user_code: 'ABCD',
        verification_uri: 'https://example.test/device', expires_in: 60, interval: 1 };
    } else if (request.url === '/v1/identity/token') {
      assert.equal(createHash('sha256').update(fields.code_verifier).digest('base64url'), pkce);
      upgraded = true;
      result = { access_token: 'access-secret', refresh_token: 'refresh-secret', token_type: 'Bearer', expires_in: 900 };
    } else {
      response.statusCode = 404;
      result = { error: 'not_found' };
    }
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(result));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  const credentials = join(directory, 'cli.json');
  const args = ['--json', '--endpoint', endpoint, '--credentials', credentials];
  try {
    const signup = await run(['signup', '--agent', ...args]);
    assert.equal(signup.code, 0, signup.stderr);
    assert.deepEqual(JSON.parse(signup.stdout), registration);
    const saved = JSON.parse(await readFile(credentials, 'utf8'));
    assert.equal(saved.agent.status, 'active');
    assert.ok(!signup.stdout.includes(saved.agent.private_key));
    if (process.platform !== 'win32') assert.equal((await stat(credentials)).mode & 0o777, 0o600);
    const count = requests.length;
    const repeated = await run(['signup', '--agent', ...args]);
    assert.equal(JSON.parse(repeated.stdout).error, 'credentials_exist');
    assert.equal(requests.length, count);
    const missingFlag = await run(['signup', ...args]);
    assert.equal(JSON.parse(missingFlag.stdout).error, 'usage_error');
    const agentRead = await run(['whoami', ...args]);
    assert.equal(JSON.parse(agentRead.stdout).error, 'login_required');
    mismatchRenewal = true;
    assert.equal(JSON.parse((await run(['refresh', ...args])).stdout).error, 'invalid_response');
    assert.equal(JSON.parse(await readFile(credentials, 'utf8')).agent.registration.certificate, 'cert-1');
    mismatchRenewal = false;
    const beforeRenewal = await readFile(credentials, 'utf8');
    for (const [field, value] of [['certificate', ''], ['birth', ''], ['expires_at', 0]]) {
      malformedRenewal = { [field]: value };
      const refused = await run(['refresh', ...args]);
      assert.equal(refused.code, 1);
      assert.equal(JSON.parse(refused.stdout).error, 'invalid_response');
      assert.equal(await readFile(credentials, 'utf8'), beforeRenewal, `${field} must preserve the receipt and key`);
    }
    malformedRenewal = undefined;
    const refreshed = await run(['refresh', ...args]);
    assert.equal(refreshed.code, 0, refreshed.stderr);
    assert.equal(JSON.parse(await readFile(credentials, 'utf8')).agent.registration.certificate, 'cert-2');
    const login = await run(['login', ...args]);
    assert.equal(login.code, 0, login.stderr);
    const upgradedCredentials = JSON.parse(await readFile(credentials, 'utf8'));
    assert.equal(upgradedCredentials.agent.private_key, saved.agent.private_key);
    assert.equal(upgradedCredentials.session.refresh_token, 'refresh-secret');
    assert.equal(upgradedCredentials.agent.registration.tier, 'oauth');
    const secondLogin = await run(['login', ...args]);
    assert.equal(secondLogin.code, 0, secondLogin.stderr);
    for (const secret of [saved.agent.private_key, 'device-secret', 'access-secret', 'refresh-secret']) {
      assert.ok(!`${signup.stdout}${signup.stderr}${login.stdout}${login.stderr}${refreshed.stdout}`.includes(secret));
    }

    failSignup = true;
    const failedPath = join(directory, 'pending.json');
    const failedArgs = ['--json', '--endpoint', endpoint, '--credentials', failedPath];
    const failed = await run(['signup', '--agent', ...failedArgs]);
    assert.equal(failed.code, 1);
    const pending = JSON.parse(await readFile(failedPath, 'utf8'));
    assert.equal(pending.agent.status, 'pending');
    assert.ok(pending.agent.private_key);
    assert.ok(!failed.stdout.includes(pending.agent.private_key));
    assert.equal((await run(['signup', '--agent', ...failedArgs])).code, 1);
    assert.equal(JSON.parse(await readFile(failedPath, 'utf8')).agent.private_key, pending.agent.private_key);
    failSignup = false;
    const recovered = await run(['signup', '--agent', ...failedArgs]);
    assert.equal(recovered.code, 0, recovered.stderr);
    assert.equal(JSON.parse(await readFile(failedPath, 'utf8')).agent.private_key, pending.agent.private_key);

    const child = spawn(binary, ['identity-mcp', '--endpoint', endpoint,
      '--credentials', join(directory, 'mcp.json')], { stdio: ['pipe', 'pipe', 'pipe'], timeout: 20_000 });
    const closed = once(child, 'close');
    const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
    let id = 0;
    async function request(method, params) {
      child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params })}\n`);
      const line = await lines.next();
      assert.equal(line.done, false);
      const response = JSON.parse(line.value);
      assert.ok(!response.error);
      return response.result;
    }
    try {
      await request('initialize', { protocolVersion: '2025-11-25' });
      const registered = await request('tools/call', { name: 'identity_signup', arguments: {} });
      assert.equal(registered.isError, false);
      assert.deepEqual(registered.structuredContent, registration);
      const renewed = await request('tools/call', { name: 'identity_refresh', arguments: {} });
      assert.equal(renewed.isError, false);
      assert.equal(renewed.structuredContent.certificate, 'cert-2');
      child.stdin.end();
      assert.equal((await closed)[0], 0);
    } finally { child.kill(); }
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});

test('cancelling a pending signup challenge allows immediate MCP shutdown', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-proof-cancel-'));
  let started, signups = 0, timer;
  const challengeReady = new Promise(resolve => { started = resolve; });
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.url === '/v1/identity/signup/challenge') {
      // Hold the response so cancellation cannot race a randomly lucky proof.
      started();
    } else {
      signups++;
      response.statusCode = 503;
      response.end(JSON.stringify({ error: 'unexpected_signup' }));
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const child = spawn(binary, ['identity-mcp', '--endpoint', `http://127.0.0.1:${server.address().port}`,
    '--credentials', join(directory, 'identity.json')], { stdio: ['pipe', 'pipe', 'pipe'], timeout: 20_000 });
  const closed = once(child, 'close');
  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const send = (id, method, params = {}) => child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
  async function response() {
    const line = await lines.next();
    assert.equal(line.done, false, stderr);
    return JSON.parse(line.value);
  }
  try {
    send(1, 'initialize', { protocolVersion: '2025-11-25' });
    assert.equal((await response()).id, 1);
    send(2, 'tools/call', { name: 'identity_signup', arguments: {} });
    await challengeReady;
    send(3, 'ping');
    assert.equal((await response()).id, 3);
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 2 } })}\n`);
    const cancelled = await response();
    assert.equal(cancelled.id, 2);
    assert.equal(cancelled.error.code, -32800);
    child.stdin.end();
    const [code] = await Promise.race([closed, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('MCP shutdown is blocked by cancelled signup')), 2000);
    })]);
    assert.equal(code, 0, stderr);
    assert.equal(signups, 0);
  } finally {
    clearTimeout(timer);
    child.kill('SIGKILL');
    await closed;
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});

test('lost committed signup recovers the same receipt and key after process restart', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-signup-receipt-'));
  const credentials = join(directory, 'identity.json');
  const registration = { user_id: 'receipt-user', org_id: 'receipt-org', key_id: 'receipt-key',
    account_id: 'receipt-account', birth: 'receipt-birth', certificate: 'receipt-certificate', tier: 'agent',
    expires_at: Math.floor(Date.now() / 1000) + 2_592_000 };
  const receipts = new Map(), keys = [], challenges = [];
  let issuedChallenge, committed = 0, invalidReceipt = true, renewal, renewals = 0, lostRenewal = true;
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    const fields = body ? JSON.parse(body) : {};
    response.setHeader('content-type', 'application/json');
    if (request.url === '/v1/identity/signup/challenge') {
      issuedChallenge = randomBytes(32).toString('base64url');
      challenges.push(issuedChallenge);
      response.end(JSON.stringify({ challenge: issuedChallenge, difficulty: 8,
        expires_at: Math.floor(Date.now() / 1000) + 600 }));
    } else if (request.url === '/v1/identity/signup') {
      assert.equal(fields.challenge, issuedChallenge);
      const key = createPublicKey({ format: 'jwk', key: { kty: 'OKP', crv: 'Ed25519', x: fields.public_key } });
      assert.ok(verify(null, Buffer.from(`acyclic-agent-signup-v1\n${fields.public_key}\n${fields.challenge}\n${fields.nonce}`),
        key, Buffer.from(fields.signature, 'base64url')));
      assert.equal(createHash('sha256').update(`${fields.challenge}.${fields.public_key}.${fields.nonce}`).digest()[0], 0);
      keys.push(fields.public_key);
      if (!receipts.has(fields.public_key)) {
        receipts.set(fields.public_key, registration);
        committed++;
        response.destroy();
        return;
      }
      response.statusCode = 200;
      response.end(JSON.stringify(invalidReceipt ? { ...registration, key_id: '' } : receipts.get(fields.public_key)));
    } else if (request.url === '/v1/identity/signup/renew') {
      const key = createPublicKey({ format: 'jwk', key: { kty: 'OKP', crv: 'Ed25519', x: keys[0] } });
      assert.equal(fields.org_id, registration.org_id);
      assert.equal(fields.key_id, registration.key_id);
      assert.ok(verify(null, Buffer.from(`acyclic-agent-renew-v1\n${fields.org_id}\n${fields.key_id}\n${fields.challenge}`),
        key, Buffer.from(fields.signature, 'base64url')));
      if (!renewal) {
        renewal = { org_id: registration.org_id, key_id: registration.key_id,
          birth: registration.birth, certificate: 'renewed-certificate', expires_at: registration.expires_at + 100 };
        renewals++;
      }
      if (lostRenewal) { lostRenewal = false; response.destroy(); return; }
      response.end(JSON.stringify(renewal));
    } else {
      response.statusCode = 404;
      response.end(JSON.stringify({ error: 'not_found' }));
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const args = ['--agent', '--json', '--endpoint', `http://127.0.0.1:${server.address().port}`, '--credentials', credentials];
  try {
    const lost = await run(['signup', ...args]);
    assert.equal(lost.code, 1);
    const pending = JSON.parse(await readFile(credentials, 'utf8'));
    assert.equal(pending.agent.status, 'pending');
    assert.equal(committed, 1, 'fake server committed before dropping the response');
    const malformed = await run(['signup', ...args]);
    assert.equal(malformed.code, 1);
    assert.equal(JSON.parse(malformed.stdout).error, 'invalid_response');
    assert.equal(JSON.parse(await readFile(credentials, 'utf8')).agent.status, 'pending');
    invalidReceipt = false;
    const recovered = await run(['signup', ...args]);
    assert.equal(recovered.code, 0, recovered.stderr);
    assert.deepEqual(JSON.parse(recovered.stdout), registration);
    const active = JSON.parse(await readFile(credentials, 'utf8'));
    assert.equal(active.agent.status, 'active');
    assert.equal(active.agent.private_key, pending.agent.private_key);
    assert.equal(committed, 1);
    assert.equal(receipts.size, 1);
    assert.equal(new Set(keys).size, 1);
    assert.equal(new Set(challenges).size, 3, 'every restart gets a fresh challenge');
    const refreshArgs = ['refresh', ...args.filter(argument => argument !== '--agent')];
    const lostRenewal = await run(refreshArgs);
    assert.equal(lostRenewal.code, 1);
    assert.equal(JSON.parse(await readFile(credentials, 'utf8')).agent.private_key, pending.agent.private_key);
    const renewed = await run(refreshArgs);
    assert.equal(renewals, 1, 'renewal retry returns the committed certificate');
    assert.equal(renewed.code, 0, renewed.stderr);
    assert.equal(JSON.parse(await readFile(credentials, 'utf8')).agent.registration.certificate, 'renewed-certificate');
    assert.equal(JSON.parse(await readFile(credentials, 'utf8')).agent.private_key, pending.agent.private_key);
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});
