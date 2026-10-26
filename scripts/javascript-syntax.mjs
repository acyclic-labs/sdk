import {readFileSync, existsSync} from 'node:fs';
import {dirname, extname, join, resolve} from 'node:path';
import * as vm from 'node:vm';

const recovery = 'node --experimental-vm-modules scripts/static-ci.mjs';

// Runtime flags are not portable: Bun's --check executes its input.
export function requireSyntaxRuntime() {
 if (process.versions.bun || typeof vm.SourceTextModule !== 'function') {
  throw new Error('step=javascript-syntax runtime requires Node with VM modules; recovery command: ' + recovery);
 }
}

function moduleSource(file) {
 const extension = extname(file);
 if (extension === '.mjs') return true;
 if (extension === '.cjs') return false;
 let directory = dirname(resolve(file));
 for (;;) {
  const manifest = join(directory, 'package.json');
  if (existsSync(manifest)) return JSON.parse(readFileSync(manifest, 'utf8')).type === 'module';
  const parent = dirname(directory);
  if (parent === directory) return false;
  directory = parent;
 }
}

// Construct parser objects only. Never link, evaluate, import, or launch a file.
export function checkJavaScript(file, source = readFileSync(file, 'utf8')) {
 requireSyntaxRuntime();
 try {
  if (moduleSource(file)) new vm.SourceTextModule(source, {identifier: file});
  else vm.compileFunction(source.replace(/^#![^\n]*/, ''),
   ['exports', 'require', 'module', '__filename', '__dirname'], {filename: file});
 } catch (error) {
  throw new Error(`step=javascript-syntax file=${JSON.stringify(file)} ${error.message}; recovery command: ${recovery}`, {cause: error});
 }
}
