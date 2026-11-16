export const rust = ["//rust/crates/actors:actors-http-routes_example"];

export function render([stdout]) {
  return {
    "typescript/packages/actors/src/routes.ts":
      `// Generated from acyclic-actors::HTTP_ROUTES. Do not edit.\nexport const HTTP_ROUTES = ${JSON.stringify(Object.fromEntries(JSON.parse(stdout)), null, 2)} as const;\n`,
  };
}
