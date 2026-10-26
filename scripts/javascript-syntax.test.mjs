import assert from 'node:assert/strict';
import {mkdtempSync, mkdirSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {checkJavaScript} from './javascript-syntax.mjs';

test('one parser process never evaluates checked files or resolves their imports', () => {
 const pid = process.pid;
 for (let i = 0; i < 1000; i++) {
  checkJavaScript(`fixture-${i}.mjs`, 'import "absent-module"; process.exit(91); throw new Error("must never execute");');
  checkJavaScript(`fixture-${i}.cjs`, '#!/usr/bin/env node\nreturn; require("absent-module"); process.exit(92);');
 }
 assert.equal(process.pid, pid);
});

test('syntax errors fail and js follows its containing package module type', () => {
 const root = mkdtempSync(join(tmpdir(), 'syntax-package-'));
 try {
  mkdirSync(join(root, 'esm'));
  writeFileSync(join(root, 'package.json'), '{"type":"commonjs"}');
  writeFileSync(join(root, 'esm/package.json'), '{"type":"module"}');
  checkJavaScript(join(root, 'common.js'), 'return; module.exports = 1;');
  checkJavaScript(join(root, 'esm/module.js'), 'export const value = await Promise.resolve(1);');
  for (const [file, source] of [
   ['invalid.mjs', 'export const = ;'],
   ['invalid.cjs', 'const = ;'],
   [join(root, 'common.js'), 'export const value = 1;'],
   [join(root, 'esm/module.js'), 'return;'],
  ]) assert.throws(() => checkJavaScript(file, source), /step=javascript-syntax.*recovery command: node/);
 } finally {
  rmSync(root, {recursive: true, force: true});
 }
});
