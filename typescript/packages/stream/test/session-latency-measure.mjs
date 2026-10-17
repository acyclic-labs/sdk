import assert from "node:assert/strict";

const WARM = 16;
const SAMPLES = 128;
export async function measure(provider, name, follow = (path, options) => provider.follow(path, options),
  { runtime = process.versions.bun ? "bun" : "node", warm = WARM, samples = SAMPLES } = {}) {
  const path = `latency/${runtime}/${name}`;
  assert.equal((await provider.append(path, [new Uint8Array([0])], { ifTail: 0n })).ok, true, `initial latency append failed for ${path}`);
  const controller = new AbortController();
  const records = follow(path, { from: 1n, signal: controller.signal })[Symbol.asyncIterator]();
  const elapsedSamples = [];
  try {
    for (let index = 0; index < warm + samples; index++) {
      const next = records.next();
      const began = performance.now();
      // Measure request-to-yield, including append validation, transport and
      // acceptance. No durability acknowledgement is awaited before the yield.
      const append = provider.append(path, [new Uint8Array([7])], { ifTail: BigInt(index + 1) });
      const item = await next;
      const elapsed = (performance.now() - began) * 1_000;
      assert.equal(item.value.sequence, BigInt(index + 1));
      assert.equal((await append).ok, true);
      if (index >= warm) elapsedSamples.push(elapsed);
    }
  } finally { controller.abort(); await records.return?.(); }
  elapsedSamples.sort((a, b) => a - b);
  return { samples, p50_us: elapsedSamples[Math.ceil(samples * .5) - 1], p99_us: elapsedSamples[Math.ceil(samples * .99) - 1] };
}

