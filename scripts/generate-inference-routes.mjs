export const rust = ["//rust/crates/inference-wasm:inference-typescript-http-routes_example"];

export function render([stdout]) {
  return {
    "typescript/packages/inference/src/routes.ts":
      `// Generated from acyclic_inference::http_codec::routes. Do not edit.\nexport const HTTP_ROUTES = ${JSON.stringify(JSON.parse(stdout), null, 2)} as const;\n`,
  };
}
