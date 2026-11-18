export const rust = ["//rust/crates/filesystem-wire:filesystem-authorization_example"];
export function render([stdout]) {
  return { "typescript/packages/filesystem/src/generated/authorization.ts": stdout };
}
