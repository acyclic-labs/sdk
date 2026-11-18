export const rust = ["//rust/crates/objects:objects-authorization_example"];
export function render([stdout]) {
  return { "typescript/packages/objects/src/generated/authorization.ts": stdout };
}
