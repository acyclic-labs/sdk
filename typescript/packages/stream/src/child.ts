import { validatePathValue } from "./contract.js";
import { StreamError, type ChildrenPageRequest, type StreamChild } from "./types.js";

export function isDirectChild(path: string, parent?: string): boolean {
  const slash = path.lastIndexOf("/");
  const selected = parent ?? "";
  return Math.max(0, slash) === selected.length && path.startsWith(selected);
}

export function validateChildCursor({ parent, after, hierarchyVersion }: Pick<ChildrenPageRequest, "parent" | "after" | "hierarchyVersion">): void {
  if (parent !== undefined) validatePathValue(parent);
  if (after !== undefined) {
    validatePathValue(after);
    if (!isDirectChild(after, parent)) {
      throw new StreamError("invalid_cursor", "child continuation must name a direct child");
    }
  }
  if (hierarchyVersion !== undefined && hierarchyVersion.byteLength !== 32) {
    throw new StreamError("invalid_cursor", "hierarchy version must be a commit identity");
  }
}

/** Checks the existing canonical path boundary and the selected direct parent. */
export function checkedChild(child: StreamChild | undefined, parent?: string): StreamChild {
  try {
    if (child === undefined) throw new Error("missing child");
    const { path } = child;
    validatePathValue(path);
    if (!isDirectChild(path, parent)) throw new Error("non-direct child");
    return { path };
  } catch { throw new StreamError("invalid_response", "invalid child discovery response"); }
}
