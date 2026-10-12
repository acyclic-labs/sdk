import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, createPrivateKey, sign } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const golden = JSON.parse(readFileSync(new URL('./fixtures/cloud-identity/golden.json', import.meta.url), 'utf8'));

function liveChallenge(recorded, nowSeconds = Math.floor(Date.now() / 1000)) {
  // Global grants challenges ten minutes from issuance; the golden's test clock is fixed.
  return { ...recorded, response: { ...recorded.response, expires_at: nowSeconds + 600 } };
}

function run(args, directory) {
  assert.ok(process.env.ACYCLIC_TEST_BINARY, 'Cargo supplies ACYCLIC_TEST_BINARY');
  return new Promise((resolve, reject) => {
    const child = spawn(process.env.ACYCLIC_TEST_BINARY, args, {
      cwd: directory, stdio: ['ignore', 'pipe', 'pipe'], timeout: 60_000,
    });
    let stdout = '', stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', code => resolve({ code, stdout, stderr }));
  });
}

async function fixture(t, handle) {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-global-contract-'));
  const failures = [], requests = [];
  const server = createServer(async (request, response) => {
    try {
      let body = '';
      for await (const chunk of request) body += chunk;
      const fields = body ? JSON.parse(body) : undefined;
      requests.push({ method: request.method, path: request.url, fields });
      if (request.method === 'POST') assert.equal(request.headers['content-type'], 'application/json');
      else assert.equal(body, '');
      const result = await handle(request, fields);
      response.writeHead(result.status, { 'content-type': 'application/json' });
      response.end(JSON.stringify(result.response));
    } catch (error) {
      failures.push(error);
      response.writeHead(500, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: 'contract_test_failed' }));
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  const credentials = join(directory, 'identity.json');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
    assert.deepEqual(failures, [], 'fake Global rejected the CLI wire contract');
  });
  return {
    directory, credentials, requests,
    call: args => run([...args, '--json', '--endpoint', endpoint, '--credentials', credentials], directory),
  };
}

test('golden fixtures identify the immutable Cloud handler tests for every operation', () => {
  assert.equal(golden.source.repository, 'acyclic-labs/cloud');
  assert.match(golden.source.commit, /^[a-f0-9]{40}$/);
  const files = new Set(golden.source.files.map(file => {
    assert.match(file.blob, /^[a-f0-9]{40}$/);
    return file.path;
  }));
  for (const name of ['device', 'token', 'whoami', 'orgs', 'challenge', 'signup', 'errors']) {
    const row = golden.cases[name];
    assert.ok(files.has(row.test_source), `${name}: missing test provenance`);
    assert.ok(files.has(row.handler_source), `${name}: missing handler provenance`);
    assert.match(row.test_source, /\.test\.mjs$/);
  }
});

test('signup challenge stays live after the recorded expiry without changing the golden', () => {
  const recorded = golden.cases.challenge;
  const original = structuredClone(recorded);
  for (const nowSeconds of [recorded.response.expires_at + 1, recorded.response.expires_at + 31_536_000]) {
    const advertised = liveChallenge(recorded, nowSeconds);
    assert.equal(advertised.response.expires_at, nowSeconds + 600);
    assert.deepEqual({ ...advertised.response, expires_at: recorded.response.expires_at }, recorded.response);
  }
  assert.deepEqual(recorded, original);
});

test('login, whoami and orgs preserve Global device/session/org JSON', { timeout: 30_000 }, async t => {
  const { device, token, whoami, orgs } = golden.cases;
  let challenge;
  const f = await fixture(t, (request, fields) => {
    if (request.url === device.path) {
      assert.equal(request.method, device.method);
      assert.equal(request.headers.authorization, undefined);
      assert.match(fields.code_challenge, /^[A-Za-z0-9_-]{43}$/);
      challenge = fields.code_challenge;
      assert.deepEqual({ ...fields, code_challenge: '<code_challenge>' }, device.request);
      return device;
    }
    if (request.url === token.path) {
      assert.equal(request.method, token.method);
      assert.equal(request.headers.authorization, undefined);
      assert.match(fields.code_verifier, /^[A-Za-z0-9._~-]{43,128}$/);
      assert.equal(createHash('sha256').update(fields.code_verifier).digest('base64url'), challenge);
      assert.deepEqual({ ...fields, code_verifier: '<code_verifier>' }, token.request);
      return token;
    }
    const row = request.url === whoami.path ? whoami : orgs;
    assert.equal(request.url, row.path);
    assert.equal(request.method, row.method);
    assert.equal(request.headers.authorization, `Bearer ${token.response.access_token}`);
    return row;
  });
  const login = await f.call(['login']);
  assert.equal(login.code, 0, login.stderr);
  assert.equal(JSON.parse(login.stdout).authenticated, true);
  assert.equal(JSON.parse(login.stderr).verification_uri_complete, device.response.verification_uri_complete);
  for (const [command, row] of [['whoami', whoami], ['orgs', orgs]]) {
    const result = await f.call([command]);
    assert.equal(result.code, 0, result.stderr);
    assert.deepEqual(JSON.parse(result.stdout), row.response);
  }
  const saved = JSON.parse(await readFile(f.credentials, 'utf8'));
  assert.equal(saved.session.access_token, token.response.access_token);
  assert.equal(saved.session.refresh_token, token.response.refresh_token);
  for (const secret of [device.response.device_code, token.response.access_token, token.response.refresh_token]) {
    assert.ok(!`${login.stdout}${login.stderr}`.includes(secret));
  }
  assert.deepEqual(f.requests.map(({ method, path }) => ({ method, path })),
    [device, token, whoami, orgs].map(({ method, path }) => ({ method, path })));
});

test('signup --agent sends the Global hashcash request and preserves its registration JSON', { timeout: 70_000 }, async t => {
  const { challenge, signup } = golden.cases;
  let registration;
  const f = await fixture(t, (request, fields) => {
    assert.equal(request.headers.authorization, undefined);
    if (request.url === challenge.path) {
      assert.equal(request.method, challenge.method);
      return liveChallenge(challenge);
    }
    assert.equal(request.url, signup.path);
    assert.equal(request.method, signup.method);
    assert.match(fields.public_key, /^[A-Za-z0-9_-]{43}$/);
    assert.match(fields.nonce, /^[A-Za-z0-9_-]{1,128}$/);
    // The pinned Global handler reads these three fields and permits extra JSON members.
    assert.deepEqual({ challenge: fields.challenge, public_key: '<public_key>', nonce: '<nonce>' }, signup.request);
    const hash = createHash('sha256').update(`${fields.challenge}.${fields.public_key}.${fields.nonce}`).digest();
    for (let bit = 0; bit < challenge.response.difficulty; bit++) {
      assert.equal(hash[bit >> 3] & (128 >> (bit % 8)), 0, `hashcash bit ${bit}`);
    }
    // Cloud's mint test uses seed 2 for this root; bind the certificate to the CLI's fresh leaf.
    const certificate = JSON.parse(Buffer.from(signup.response.certificate.split('.')[0], 'base64url'));
    certificate.public_key = fields.public_key;
    const body = Buffer.from(JSON.stringify(certificate)).toString('base64url');
    const root = createPrivateKey({
      key: Buffer.concat([Buffer.from('302e020100300506032b657004220420', 'hex'), Buffer.alloc(32, 2)]),
      format: 'der', type: 'pkcs8',
    });
    const signature = sign(null, Buffer.from(`acyclic-account-key-v1.${body}`), root).toString('base64url');
    registration = { ...signup.response, certificate: `${body}.${signature}` };
    return { ...signup, response: registration };
  });
  const result = await f.call(['signup', '--agent']);
  assert.equal(result.code, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout), registration);
  const saved = JSON.parse(await readFile(f.credentials, 'utf8'));
  assert.deepEqual(saved.agent.registration, registration);
  assert.ok(saved.agent.private_key);
  assert.ok(!`${result.stdout}${result.stderr}`.includes(saved.agent.private_key));
  assert.deepEqual(f.requests.map(({ method, path }) => ({ method, path })),
    [challenge, signup].map(({ method, path }) => ({ method, path })));
});

test('login preserves the Global device-denial error without saving a session', { timeout: 20_000 }, async t => {
  const { device, token, errors } = golden.cases;
  const denied = errors.responses.find(value => value.error === 'access_denied');
  assert.ok(denied);
  const f = await fixture(t, request => {
    if (request.url === device.path) return device;
    assert.equal(request.url, token.path);
    assert.equal(request.method, token.method);
    return { status: 400, response: denied };
  });
  const result = await f.call(['login']);
  assert.equal(result.code, 1);
  assert.equal(JSON.parse(result.stdout).error, denied.error);
  await assert.rejects(readFile(f.credentials), { code: 'ENOENT' });
  assert.equal(f.requests.length, 2);
});
