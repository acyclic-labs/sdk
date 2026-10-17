import { validatePathValue } from "./contract.js";
import { StreamError, type StreamChild } from "./types.js";

/** Checks the existing canonical path boundary and the selected direct parent. */
export function checkedChild(child: StreamChild | undefined, parent?: string): StreamChild {
  try {
    if (child === undefined) throw new Error("missing child");
    validatePathValue(child.path);
    const slash = child.path.lastIndexOf("/");
    if ((slash === -1 ? "" : child.path.slice(0, slash)) !== (parent ?? "")) throw new Error("non-direct child");
    return { path: child.path };
  } catch { throw new StreamError("invalid_response", "invalid child discovery response"); }
}
