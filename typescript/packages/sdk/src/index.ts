export * as harness from "@acyclic-labs/harness";
export * as filesystem from "@acyclic-labs/fs";
export * as stream from "@acyclic-labs/stream";
export * as objects from "@acyclic-labs/objects";
export * as machines from "@acyclic-labs/machines";
export * as inference from "@acyclic-labs/inference";

/** Recursively reduces a balanced workload. */
export async function recursiveSum(values: readonly number[], leafSize = 4): Promise<number> {
  if (values.length <= Math.max(1, leafSize)) return values.reduce((sum, value) => sum + value, 0);
  const midpoint = Math.floor(values.length / 2);
  const [left, right] = await Promise.all([recursiveSum(values.slice(0, midpoint), leafSize), recursiveSum(values.slice(midpoint), leafSize)]);
  return left + right;
}
