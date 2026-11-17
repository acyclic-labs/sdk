import { renderTypes } from "./filesystem-napi-types.mjs";
export const crates = ["acyclic-auth", "acyclic-stream-napi"];
export const render = (_, root) => renderTypes(root, "stream", crates);
