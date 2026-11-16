export const rust = ["//rust/crates/actors:actors-authorization_example"];
export function render([stdout]) {
  return { "typescript/packages/actors/src/generated/authorization.ts": stdout };
}
