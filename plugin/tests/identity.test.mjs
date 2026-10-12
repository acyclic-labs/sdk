import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { link, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { once } from 'node:events';
import { promisify } from 'node:util';

const binary = process.env.ACYCLIC_TEST_BINARY;
assert.ok(binary, 'set ACYCLIC_TEST_BINARY to the built native acyclic binary');

function run(args, cwd) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'], timeout: 20_000 });
    let stdout = '', stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', code => resolve({ code, stdout, stderr }));
  });
}

test('native JSON login, reads and explicit refresh share rotated credentials', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-identity-'));
  const credentials = join(directory, 'identity.json');
  const requests = [];
  let challenge;
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    const fields = body ? JSON.parse(body) : {};
    requests.push({ method: request.method, path: request.url, fields,
      bearer: request.headers.authorization });
    let result;
    if (request.url === '/v1/identity/device/code') {
      assert.equal(request.headers['content-type'], 'application/json');
      assert.equal(fields.client_kind, 'cli');
      assert.match(fields.code_challenge, /^[\w-]{43}$/);
      challenge = fields.code_challenge;
      result = { device_code: 'device-secret', user_code: 'ABCD',
        verification_uri: 'https://example.test/device', expires_in: 60, interval: 1 };
    } else if (request.url === '/v1/identity/token') {
      const refresh = fields.grant_type === 'refresh_token';
      if (!refresh) assert.equal(createHash('sha256').update(fields.code_verifier).digest('base64url'), challenge);
      result = { access_token: refresh ? 'access-2' : 'access-1',
        refresh_token: refresh ? 'refresh-2' : 'refresh-1', token_type: 'Bearer', expires_in: 900 };
    } else if (request.url === '/v1/identity/session') result = { user: { id: 'user-1' } };
    else if (request.url === '/v1/identity/orgs') result = { orgs: [{ id: 'org-1' }] };
    else { response.statusCode = 404; result = { error: 'not_found' }; }
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(result));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const args = ['--json', '--endpoint', `http://127.0.0.1:${server.address().port}`,
    '--credentials', credentials];
  try {
    const login = await run(['login', ...args, '--credentials', 'identity.json'], directory);
    assert.equal(login.code, 0, login.stderr);
    assert.equal(JSON.parse(login.stdout).authenticated, true);
    assert.equal(JSON.parse(login.stderr).user_code, 'ABCD');
    assert.deepEqual(JSON.parse((await run(['whoami', ...args])).stdout), { user: { id: 'user-1' } });
    assert.deepEqual(JSON.parse((await run(['orgs', ...args])).stdout), { orgs: [{ id: 'org-1' }] });
    const refresh = await run(['refresh', ...args]);
    assert.equal(refresh.code, 0, refresh.stderr);
    const saved = JSON.parse(await readFile(credentials, 'utf8'));
    assert.equal(saved.session.refresh_token, 'refresh-2');
    assert.deepEqual(requests.map(({ path }) => path), [
      '/v1/identity/device/code', '/v1/identity/token', '/v1/identity/session',
      '/v1/identity/orgs', '/v1/identity/token',
    ]);
    assert.equal(requests[1].fields.device_code, 'device-secret');
    assert.equal(requests[2].bearer, 'Bearer access-1');
    assert.equal(requests[4].fields.refresh_token, 'refresh-1');
    for (const secret of ['access-1', 'refresh-1', 'access-2', 'refresh-2', 'device-secret']) {
      assert.ok(!`${login.stdout}${login.stderr}${refresh.stdout}${refresh.stderr}`.includes(secret));
    }
    const failure = await run(['whoami', ...args, '--unexpected']);
    assert.equal(failure.code, 1);
    assert.equal(JSON.parse(failure.stdout).error, 'usage_error');
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});

test('killing refresh after server rotation cannot replay the spent token', { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-interrupted-refresh-'));
  const credentials = join(directory, 'identity.json');
  let received, requests = 0, child;
  const rotated = new Promise(resolve => { received = resolve; });
  const server = createServer(async request => {
    let body = '';
    for await (const chunk of request) body += chunk;
    assert.deepEqual(JSON.parse(body), { grant_type: 'refresh_token', refresh_token: 'spent-refresh' });
    requests++;
    received();
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  const args = ['--json', '--endpoint', endpoint, '--credentials', credentials];
  try {
    await writeFile(credentials, JSON.stringify({ endpoint, session: {
      access_token: 'access', refresh_token: 'spent-refresh', expires_at: 0 } }));
    child = spawn(binary, ['refresh', ...args], { stdio: 'ignore', timeout: 15_000 });
    const closed = once(child, 'close');
    await rotated;
    child.kill('SIGKILL');
    await closed;
    const restarted = await run(['refresh', ...args]);
    assert.equal(restarted.code, 1);
    assert.equal(JSON.parse(restarted.stdout).error, 'login_required');
    assert.equal(requests, 1, 'restarted CLI must not replay the rotating token');
  } finally {
    child?.kill();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});

test('credential symlinks cannot send a refresh while leaving the original token', { skip: process.platform === 'win32' }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-identity-alias-'));
  const credentials = join(directory, 'identity.json'), alias = join(directory, 'alias.json');
  let requests = 0;
  const server = createServer((request, response) => {
    requests++;
    response.end(JSON.stringify({ error: 'unexpected_request' }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  try {
    await writeFile(credentials, JSON.stringify({ endpoint, session: {
      access_token: 'access', refresh_token: 'unused-refresh', expires_at: 0 } }));
    await symlink(credentials, alias);
    const refused = await run(['refresh', '--json', '--endpoint', endpoint, '--credentials', alias]);
    assert.equal(refused.code, 1);
    assert.equal(JSON.parse(refused.stdout).error, 'credential_error');
    assert.equal(requests, 0);
    assert.equal(JSON.parse(await readFile(credentials, 'utf8')).session.refresh_token, 'unused-refresh');
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});

test('credential hard links reject both Windows and Unix paths before refreshing', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-identity-hardlink-'));
  const credentials = join(directory, 'identity.json'), alias = join(directory, 'alias.json');
  let requests = 0;
  const server = createServer((request, response) => {
    requests++;
    response.end(JSON.stringify({ error: 'unexpected_request' }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  try {
    const saved = JSON.stringify({ endpoint, session: {
      access_token: 'access', refresh_token: 'unused-refresh', expires_at: 0 } });
    await writeFile(credentials, saved);
    await link(credentials, alias);
    const results = await Promise.all([credentials, alias].map(path =>
      run(['refresh', '--json', '--endpoint', endpoint, '--credentials', path])));
    for (const refused of results) {
      assert.equal(refused.code, 1);
      assert.equal(JSON.parse(refused.stdout).error, 'credential_error');
    }
    assert.equal(requests, 0);
    assert.equal(await readFile(credentials, 'utf8'), saved);
    assert.equal(await readFile(alias, 'utf8'), saved);
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});


test('credential sidecar names cannot replace another store or its held lock', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-identity-sidecars-'));
  const credentials = join(directory, 'session');
  let consume, release, requests = 0, child;
  const consumed = new Promise(resolve => { consume = resolve; });
  const released = new Promise(resolve => { release = resolve; });
  const server = createServer(async (request, response) => {
    for await (const _ of request) { /* drain */ }
    requests++;
    consume();
    await released;
    response.end(JSON.stringify({ access_token: 'rotated-access', refresh_token: 'rotated-refresh',
      token_type: 'Bearer', expires_in: 900 }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  const args = ['--json', '--endpoint', endpoint];
  try {
    const saved = JSON.stringify({ endpoint, session: {
      access_token: 'access', refresh_token: 'refresh', expires_at: 0 } });
    await writeFile(credentials, saved);
    // A rejected sidecar store must leave even pre-existing contents untouched.
    const other = join(directory, 'other.next');
    await writeFile(other, saved);
    const refused = await run(['login', ...args, '--credentials', other]);
    assert.equal(refused.code, 1);
    assert.equal(JSON.parse(refused.stdout).error, 'configuration_error');
    assert.equal(await readFile(other, 'utf8'), saved);
    child = spawn(binary, ['refresh', ...args, '--credentials', credentials],
      { stdio: 'ignore', timeout: 15_000 });
    const closed = once(child, 'close');
    await consumed;
    for (const suffix of ['.lock', '.next', '.LOCK', '.NeXt']) {
      const result = await run(['login', ...args, '--credentials', credentials + suffix]);
      assert.equal(result.code, 1);
      assert.equal(JSON.parse(result.stdout).error, 'configuration_error');
    }
    const busy = await run(['refresh', ...args, '--credentials', credentials]);
    assert.equal(JSON.parse(busy.stdout).error, 'credentials_busy');
    assert.equal(requests, 1, 'sidecar logins must not contact Global or replace the held lock');
    release();
    assert.equal((await closed)[0], 0);
    assert.equal(JSON.parse(await readFile(credentials, 'utf8')).session.refresh_token, 'rotated-refresh');
  } finally {
    release();
    child?.kill();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});

test('Windows short-name credentials share the long-name refresh lock',
  { skip: process.platform !== 'win32', timeout: 20_000 }, async t => {
    const directory = await mkdtemp(join(tmpdir(), 'acyclic-shortname-'));
    const credentials = join(directory, 'identity-credentials-long-name.json');
    let consume, release, requests = 0, child;
    const consumed = new Promise(resolve => { consume = resolve; });
    const released = new Promise(resolve => { release = resolve; });
    const server = createServer(async (request, response) => {
      for await (const _ of request) { /* drain */ }
      requests++;
      consume();
      await released;
      response.end(JSON.stringify({ access_token: 'rotated', refresh_token: 'rotated-refresh',
        token_type: 'Bearer', expires_in: 900 }));
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const endpoint = `http://127.0.0.1:${server.address().port}`;
    const args = ['--json', '--endpoint', endpoint];
    try {
      await writeFile(credentials, JSON.stringify({ endpoint, session: {
        access_token: 'access', refresh_token: 'refresh', expires_at: 0 } }));
      const { stdout } = await promisify(execFile)('cmd.exe', ['/d', '/c',
        'for %I in ("%ACYCLIC_TEST_CREDENTIALS%") do @echo %~sI'],
        { env: { ...process.env, ACYCLIC_TEST_CREDENTIALS: credentials } });
      const alias = stdout.trim();
      if (!/[~][0-9]/.test(alias.split(/[\\/]/).at(-1))) {
        t.skip('NTFS 8.3 filenames are disabled on this test volume');
        return;
      }
      assert.deepEqual(await readFile(alias), await readFile(credentials));
      child = spawn(binary, ['refresh', ...args, '--credentials', alias],
        { stdio: 'ignore', timeout: 15_000 });
      const closed = once(child, 'close');
      await consumed;
      const busy = await run(['refresh', ...args, '--credentials', credentials]);
      assert.equal(JSON.parse(busy.stdout).error, 'credentials_busy');
      assert.equal(requests, 1, 'long and short names must never dispatch the same refresh twice');
      release();
      assert.equal((await closed)[0], 0);
      assert.equal(JSON.parse(await readFile(credentials, 'utf8')).session.refresh_token, 'rotated-refresh');
    } finally {
      release();
      child?.kill();
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
      await rm(directory, { recursive: true, force: true });
    }
  });
