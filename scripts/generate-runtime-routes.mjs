export const rust = [
  ["acyclic-actors", "example", "actors-http-routes"],
  ["acyclic-inference-wasm", "example", "inference-typescript-http-routes"],
];

const routes = (family, stdout) =>
  `// Generated from acyclic-${family}::HTTP_ROUTES. Do not edit.\nexport const HTTP_ROUTES = ${JSON.stringify(Object.fromEntries(JSON.parse(stdout)), null, 2)} as const;\n`;

export function render([actors, inference]) {
  return {
    "typescript/packages/actors/src/routes.ts": routes("actors", actors),
    "typescript/packages/inference/src/routes.ts": `// Generated from acyclic_inference::http_codec::routes. Do not edit.\nexport const HTTP_ROUTES = ${JSON.stringify(JSON.parse(inference), null, 2)} as const;\n`,
  };
}
