export const rust = [["acyclic-workers", "example", "workers-authorization"]];
export function render([stdout]) {
  return { "typescript/packages/workers/src/generated/authorization.ts": stdout };
}
