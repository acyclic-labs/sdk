export const rust = ["//rust/crates/stream:stream-authorization_example"];
export function render([stdout]) {
  return { "typescript/packages/stream/src/generated/authorization.ts": stdout };
}
