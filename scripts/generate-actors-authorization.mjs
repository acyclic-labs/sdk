export const rust = [["acyclic-actors", "example", "actors-authorization"]];
export function render([stdout]) {
  return { "typescript/packages/actors/src/generated/authorization.ts": stdout };
}
