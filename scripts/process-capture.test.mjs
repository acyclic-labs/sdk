import assert from 'node:assert/strict';
import { mkdtempSync, openSync, closeSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { captureCommand, commandOutput } from './process-capture.mjs';

function isolatedCapture(run) {
  const directory = mkdtempSync(join(tmpdir(), 'acyclic-capture-test-'));
  const names = ['TMPDIR', 'TMP', 'TEMP'];
  const previous = names.map(name => process.env[name]);
  for (const name of names) process.env[name] = directory;
  try {
    run(directory);
    assert.deepEqual(readdirSync(directory), [], 'capture releases every spool');
  } finally {
    names.forEach((name, index) => {
      if (previous[index] === undefined) delete process.env[name];
      else process.env[name] = previous[index];
    });
    rmSync(directory, { recursive: true, force: true });
  }
}

test('captures complete streams beyond the former compiler output ceilings', () => isolatedCapture(() => {
  const size = 17 * 1024 * 1024;
  const result = captureCommand(process.execPath, ['-e', `
    const fs = require('node:fs');
    fs.writeFileSync(1, Buffer.alloc(${size}, 0x61));
    fs.writeFileSync(2, Buffer.alloc(${size}, 0x62));
  `]);
  assert.equal(result.status, 0);
  assert.equal(result.error, undefined);
  assert.deepEqual(result.stdout, Buffer.alloc(size, 0x61));
  assert.deepEqual(result.stderr, Buffer.alloc(size, 0x62));
  assert.equal(result.output[1], result.stdout);
  assert.equal(result.output[2], result.stderr);
}));

test('preserves input, encoding, status and caller-owned file descriptors', () => isolatedCapture(directory => {
  const bytes = Buffer.alloc(2 * 1024 * 1024, 0x7f);
  const result = captureCommand(process.execPath, ['-e', `
    const fs = require('node:fs');
    fs.writeFileSync(1, fs.readFileSync(0));
    process.exitCode = 7;
  `], { input: bytes });
  assert.equal(result.status, 7);
  assert.deepEqual(result.stdout, bytes);
  assert.equal(commandOutput(process.execPath, ['-e', 'process.stdout.write("é🦀")'], { encoding: 'utf8' }), 'é🦀');
  const file = join(directory, 'caller-owned');
  const fd = openSync(file, 'wx');
  try {
    const inherited = captureCommand(process.execPath, ['-e', 'process.stdout.write("file")'], { stdio: ['ignore', fd, 'ignore'] });
    assert.equal(inherited.status, 0);
    assert.equal(inherited.stdout, null);
  } finally { closeSync(fd); }
  assert.equal(readFileSync(file, 'utf8'), 'file');
  rmSync(file);
}));

test('retains spawn and exit failures with complete stderr', () => isolatedCapture(() => {
  const missing = captureCommand('acyclic-command-that-does-not-exist', []);
  assert.equal(missing.error && 'code' in missing.error ? missing.error.code : undefined, 'ENOENT');
  assert.equal(missing.status, null);
  assert.throws(() => commandOutput(process.execPath, ['-e', 'process.stderr.write("failure"); process.exitCode=9'], { encoding: 'utf8' }), error => error instanceof Error && 'status' in error && error.status === 9 && 'stderr' in error && error.stderr === 'failure');
}));

test('retains the configured timeout and partial diagnostics', () => isolatedCapture(() => {
  const result = captureCommand(process.execPath, ['-e', 'require("node:fs").writeSync(1,"started"); Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0)'], { encoding: 'utf8', timeout: 2000, killSignal: 'SIGKILL' });
  assert.equal(result.error && 'code' in result.error ? result.error.code : undefined, 'ETIMEDOUT');
  assert.equal(result.stdout, 'started');
}));
