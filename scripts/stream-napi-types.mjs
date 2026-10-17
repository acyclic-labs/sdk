import { renderTypes } from "./filesystem-napi-types.mjs";
export const render = (_, root) => renderTypes(root, "stream", ["acyclic-stream-napi"]);
