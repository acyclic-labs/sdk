import {writeFileSync} from 'node:fs';
import {relative} from 'node:path';
import {inspect} from 'node:util';

// Node's event stream retains test ancestry and original assertion diagnostics.
export default async function* report(events) {
 const ancestry = [];
 let summary;
 const stable = value => String(value).replaceAll('\r', '\\r').replaceAll('\n', '\\n');
 for await (const {type, data} of events) {
  if (type === 'test:start') { ancestry.length = data.nesting; ancestry.push(data.name); }
  if (type === 'test:fail' && !data.todo && !data.skip) {
   const file = relative(process.cwd(), data.file ?? data.name).replaceAll('\\', '/');
   const name = [...ancestry.slice(0, data.nesting), data.name === data.file ? file : data.name].join(' > ');
   yield `NODE_TEST_FAIL ${stable(file)}::${stable(name)}\n`;
   yield inspect(data.details.error, {depth: null, colors: false}) + '\n';
  }
  if (type === 'test:stdout' || type === 'test:stderr') yield data.message;
  if (type === 'test:summary' && !data.file) summary = data;
 }
 if (!summary) throw new Error('step=collect Node summary; recovery command: node scripts/node-tests.mjs --all');
 const {tests, passed, failed, cancelled, skipped, todo} = summary.counts;
 const path = process.env.SDK_NODE_TEST_REPORT;
 if (!path) throw new Error('step=locate Node report; recovery command: node scripts/node-tests.mjs --all');
 writeFileSync(path, JSON.stringify({tests, passed, failed: failed + cancelled, skipped: skipped + todo}));
}
