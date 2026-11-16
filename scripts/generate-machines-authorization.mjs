export const rust = ["//rust/crates/machines:machines-authorization_example"];
export function render([stdout]) {
  return { "typescript/packages/machines/src/generated/authorization.ts": stdout };
}
