export * from "@acyclic/filesystem";
export * from "@acyclic/stream";
export * from "@acyclic/objects";
export * from "@acyclic/machines";
export * from "@acyclic/inference";

/** Recursively reduces a balanced workload. */
export async function recursiveSum(values: readonly number[], leafSize = 4): Promise<number> {
  if (values.length <= Math.max(1, leafSize)) return values.reduce((sum, value) => sum + value, 0);
  const midpoint = Math.floor(values.length / 2);
  const [left, right] = await Promise.all([recursiveSum(values.slice(0, midpoint), leafSize), recursiveSum(values.slice(midpoint), leafSize)]);
  return left + right;
}
