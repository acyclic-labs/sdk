export const rust = ["//rust/crates/workers:workers-authorization_example"];
export function render([stdout]) {
  return { "typescript/packages/workers/src/generated/authorization.ts": stdout };
}
