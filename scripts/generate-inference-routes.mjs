export const rust = [["acyclic-inference-wasm", "example", "inference-typescript-http-routes"]];

export function render([stdout]) {
  return {
    "typescript/packages/inference/src/routes.ts":
      `// Generated from acyclic_inference::http_codec::routes. Do not edit.\nexport const HTTP_ROUTES = ${JSON.stringify(JSON.parse(stdout), null, 2)} as const;\n`,
  };
}
