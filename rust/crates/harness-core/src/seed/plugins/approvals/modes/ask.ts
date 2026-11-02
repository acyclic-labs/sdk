import type { BeforeHook } from "acyclic";

export default (() => ({ ask: "Approve this request?", to: "person" })) satisfies BeforeHook;
