import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {stripTypeScriptTypes} from 'node:module';
import test from 'node:test';

const source = await readFile(new URL(
  '../rust/crates/harness-core/src/seed/plugins/compaction/hooks/after-usage.ts',
  import.meta.url,
), 'utf8');

async function load(text) {
  const javascript = stripTypeScriptTypes(text);
  return (await import(`data:text/javascript;base64,${Buffer.from(javascript).toString('base64')}`)).default;
}

const hook = await load(source);
const config = Object.freeze({notify: 0.75, reuse: 'ancestors'});
const usage = (inputTokens, capacity = 1000) => Object.freeze({
  revision: 7, model: 'selected', inputTokens, cachedTokens: 500,
  outputTokens: 10, capacity, cost: 0.001,
  compaction: Object.freeze({summariseNow: 0.012, perCallIfNot: 0.003, autoTrimAtTokens: 1000}),
});
const event = (added, settings = config) => ({
  delivery: {kind: 'log', path: 'live/agents/self/usage.jsonl', count: added.length, added},
  config: settings, key: 'usage:7',
});

test('the seed hook explains admitted pressure without waking or inventing prices', () => {
  for (let input = 0; input <= 1000; input += 25) {
    const delivered = event(Object.freeze([usage(input)]));
    const changes = hook(delivered);
    assert.equal(changes.length, input >= 750 ? 1 : 0);
    if (!changes.length) continue;
    const {notice} = changes[0];
    assert.equal(notice.wake, false);
    assert.equal(notice.once, 'compaction-0.75');
    assert.match(notice.text, /\$0\.012/);
    assert.match(notice.text, /\$0\.003/);
    assert.match(notice.text, /1000 tokens/);
    assert.deepEqual(hook(delivered), changes);
  }
  const unknown = {...usage(800), compaction: {
    summariseNow: null, perCallIfNot: null, autoTrimAtTokens: 900,
  }};
  const [{notice}] = hook(event([unknown]));
  assert.equal(notice.text.match(/unknown/g)?.length, 2);
  assert.doesNotMatch(notice.text, /\$/);
  assert.deepEqual(hook(event([])), []);
  assert.deepEqual(hook(event([usage(800, 0)])), []);
  assert.deepEqual(hook(event([usage(900)], {...config, notify: 0.95})), []);
  assert.equal(hook(event([usage(0)], {...config, notify: 0})).length, 1);
  const unreadable = new Proxy([], {get() { throw new Error('disabled hook read its delivery'); }});
  assert.deepEqual(hook({delivery: {kind: 'log', added: unreadable}, config: {...config, notify: null}}), []);
  assert.deepEqual(hook({delivery: {kind: 'file'}, config}), []);
});

test('selected Usage work is fixed and a history-copy mutation fails the same control', async () => {
  function selectedOnly(run, count) {
    let reads = 0;
    const page = new Array(count);
    page[count - 1] = usage(800);
    const added = new Proxy(Object.freeze(page), {
      has(target, key) {
        if (typeof key === 'string' && /^\d+$/.test(key)) {
          assert.equal(Number(key), count - 1, 'hook inspected old Usage');
          reads++;
        }
        return Reflect.has(target, key);
      },
      get(target, key, receiver) {
        if (typeof key === 'string' && /^\d+$/.test(key)) {
          assert.equal(Number(key), count - 1, 'hook inspected old Usage');
          reads++;
        }
        return Reflect.get(target, key, receiver);
      },
    });
    assert.equal(run(event(added)).length, 1);
    assert.equal(reads, 1, 'hook reread selected Usage');
  }
  for (const count of [1, 100000]) selectedOnly(hook, count);

  // Change only the selected-record expression in the real seed program.
  const copied = source.replace('.at(-1)', '.slice().at(-1)');
  assert.notEqual(copied, source);
  const mutant = await load(copied);
  assert.throws(() => selectedOnly(mutant, 100000), /hook inspected old Usage/);
});
