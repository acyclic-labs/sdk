// Seeds are original outputs of build-product.mjs and package-products.mjs.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { buildManifest, productManifest } from './verify-products.mjs';

const [products, build] = process.argv.slice(2).map(file => readFileSync(file));
const original = JSON.parse(products);
const { version, source } = original;
const expected = ['graphcoder', 'acyclic-plugin'].flatMap(product =>
  ['linux-x64', 'linux-arm64'].map(platform => `${product}-${version}-${platform}.tar.gz`)).sort();

function check(bytes, kind) {
  let admitted;
  try {
    admitted = kind === 'products' ? productManifest(bytes, version, source) : buildManifest(bytes, source);
  } catch { return; }
  const value = JSON.parse(bytes);
  assert.equal(value.format, 1, `${kind}: format admission lost`);
  assert.equal(value.source, source, `${kind}: source admission lost`);
  if (kind === 'products') {
    assert.equal(value.version, version);
    assert.deepEqual([...value.assets].sort(), expected);
    assert.deepEqual(admitted, [...expected, 'PRODUCTS.json'].sort());
  } else {
    assert.equal(value.dirty, false);
    for (const name of ['graphcoder', 'acyclic']) assert.match(value.hashes[name], /^[0-9a-f]{64}$/);
  }
}

let state = 71;
const random = () => (state = (Math.imul(state, 1664525) + 1013904223) >>> 0);
for (const [kind, bytes] of [['products', products], ['build', build]]) {
  // Original positive must enter the decoder, independently of mutation refusals.
  if (kind === 'products') productManifest(bytes, version, source);
  else buildManifest(bytes, source);
  check(bytes, kind);
  for (const [field, replacement] of [['format', 0], ['source', '0'.repeat(40)], ['version', '0.0.0'], ['dirty', true], ['assets', []], ['hashes', {}]]) {
    check(Buffer.from(JSON.stringify({ ...JSON.parse(bytes), [field]: replacement })), kind);
  }
  for (let iteration = 0; iteration < 1024; iteration++) {
    const mutated = Buffer.from(bytes);
    mutated[random() % mutated.length] ^= 1 << (random() % 8);
    check(mutated, kind);
    check(mutated.subarray(0, random() % mutated.length), kind);
  }
}
console.log('product/build original manifest properties PASS: seed 71, 4096 byte mutations plus semantic omissions');
