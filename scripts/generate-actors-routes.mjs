export const rust = [["acyclic-actors", "example", "actors-http-routes"]];

export function render([stdout]) {
  return {
    "typescript/packages/actors/src/routes.ts":
      `// Generated from acyclic-actors::HTTP_ROUTES. Do not edit.\nexport const HTTP_ROUTES = ${JSON.stringify(Object.fromEntries(JSON.parse(stdout)), null, 2)} as const;\n`,
  };
}
