export * as filesystem from "@acyclic/fs";
export * as stream from "@acyclic/stream";
export * as objects from "@acyclic/objects";
export * as machines from "@acyclic/machines";
export * as inference from "@acyclic/inference";

/** Recursively reduces a balanced workload. */
export async function recursiveSum(values: readonly number[], leafSize = 4): Promise<number> {
  if (values.length <= Math.max(1, leafSize)) return values.reduce((sum, value) => sum + value, 0);
  const midpoint = Math.floor(values.length / 2);
  const [left, right] = await Promise.all([recursiveSum(values.slice(0, midpoint), leafSize), recursiveSum(values.slice(midpoint), leafSize)]);
  return left + right;
}
