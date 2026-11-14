import { renderTypes } from "./filesystem-napi-types.mjs";
// Actors' TypeScript loads its N-API binding untyped, so only the native family build reads these declarations. A
// selected generation (a WASM build, for one) omits them unless it asks for the native outputs.
export const nativeOnly = true;
export const render = (_, root) => renderTypes(root, "actors", ["acyclic-actors-napi"]);
