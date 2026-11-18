// Source-only command for consumers that resolve the SDK's public Bazel runtime
// target. The caller supplies the parser file from that same target, never npm.
import {readFileSync} from 'node:fs';
import {registerHooks} from 'node:module';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';

const [parser, ...extra] = process.argv.slice(2);
if (!parser || extra.length) throw Error('Expected the pinned TypeScript parser file');
const projectUrl = new URL('./project.mjs', import.meta.url).href;
const hooks = registerHooks({resolve(specifier, context, next) {
 if (specifier === 'typescript-parser' && context.parentURL === projectUrl)
  return next(pathToFileURL(resolve(parser)).href, context);
 return next(specifier, context);
}});
let analyze;
try { ({newProjectPollingSites: analyze} = await import(projectUrl)); }
finally { hooks.deregister(); }

const request = JSON.parse(readFileSync(0, 'utf8'));
function sources(entries) {
 if (!Array.isArray(entries)) throw Error('Expected source entries');
 const result = new Map();
 for (const entry of entries) {
  if (!Array.isArray(entry) || entry.length !== 2 || entry.some(value => typeof value !== 'string') || result.has(entry[0]))
   throw Error('Expected unique file/source pairs');
  result.set(entry[0], entry[1]);
 }
 return result;
}
process.stdout.write(JSON.stringify(analyze(sources(request.before), sources(request.after), request.dispositions ?? [], request.policy ?? {})) + '\n');
