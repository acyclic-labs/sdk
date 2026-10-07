export const rust = [
  ["acyclic-actors", "example", "actors-http-routes"],
  ["acyclic-workers", "example", "workers-http-routes"],
  ["acyclic-workers", "example", "workers-module-contract"],
];

const routes = (family, stdout) =>
  `// Generated from acyclic-${family}::HTTP_ROUTES. Do not edit.\nexport const HTTP_ROUTES = ${JSON.stringify(Object.fromEntries(JSON.parse(stdout)), null, 2)} as const;\n`;

export function render([actors, workers, workersModule]) {
  return {
    "typescript/packages/actors/src/routes.ts": routes("actors", actors),
    "typescript/packages/workers/src/routes.ts": routes("workers", workers),
    "typescript/packages/workers/src/module-contract.ts": workersModule,
  };
}
