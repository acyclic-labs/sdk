import { gcAndSweep, heapStats } from "bun:jsc";
const { SnapshotStore } = await import(process.env.HARNESS_SNAPSHOT_MODULE ??
  new URL("../src/client-snapshot.ts", import.meta.url).href) as typeof import("../src/client-snapshot.js");

const iterations = 100_000;
const censusIterations = 256;
let checksum = 0;

function timing(publish: (value: number) => void): number {
  const run = (): number => {
    const start = performance.now();
    for (let i = 0; i < iterations; i++) publish(i);
    return (performance.now() - start) * 1_000_000 / iterations;
  };
  const warmStart = performance.now();
  while (performance.now() - warmStart < 250) run();
  const samples = Array.from({ length: 7 }, run);
  samples.sort((left, right) => left - right);
  return samples[3]!;
}

function census(publish: (value: number) => void) {
  gcAndSweep();
  const before = heapStats();
  for (let i = 0; i < censusIterations; i++) publish(-i);
  const after = heapStats();
  return {
    arrays: (after.objectTypeCounts.Array ?? 0) - (before.objectTypeCounts.Array ?? 0),
    functions: (after.objectTypeCounts.Function ?? 0) - (before.objectTypeCounts.Function ?? 0),
  };
}

const overhead = census(() => {});

for (const subscribers of [0, 1, 8, 64]) {
  const store = new SnapshotStore(0);
  let current = 0;
  const floor = {
    getSnapshot: () => current,
    publish: (value: number) => {
      if (Object.is(current, value)) return;
      current = value;
      for (const callback of callbacks) callback();
    },
  };
  const callbacks: (() => void)[] = [];
  for (let i = 0; i < subscribers; i++) {
    store.subscribe(() => { checksum += store.getSnapshot(); });
    callbacks.push(() => { checksum += floor.getSnapshot(); });
  }
  const floorNs = timing(floor.publish);
  const publishNs = timing(store.publish);
  const objects = census(store.publish);
  console.log(JSON.stringify({
    subscribers, floorNs, publishNs, censusIterations,
    arraysPerPublication: (objects.arrays - overhead.arrays) / censusIterations,
    functionsPerPublication: (objects.functions - overhead.functions) / censusIterations,
  }));
  store.dispose();
}
console.log(JSON.stringify({ checksum }));
