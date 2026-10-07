import { join } from "node:path";
import { pathToFileURL } from "node:url";

const root = process.env.ACYCLIC_SDK_ROOT;
if (!root) throw new Error("ACYCLIC_SDK_ROOT must name the SDK checkout");
const bun = join(root, "node_modules", ".bun");
const targets = new Map([
  ["@bufbuild/protobuf", join(bun, "@bufbuild+protobuf@2.15.0", "node_modules", "@bufbuild", "protobuf", "dist", "esm", "index.js")],
  ["@connectrpc/connect", join(bun, "@connectrpc+connect@2.1.1+432d72b9ceda49ac", "node_modules", "@connectrpc", "connect", "dist", "esm", "index.js")],
  ["@connectrpc/connect-node", join(bun, "@connectrpc+connect-node@2.1.1+362e60f3a5b2a079", "node_modules", "@connectrpc", "connect-node", "dist", "esm", "index.js")],
]);

export function resolve(specifier, context, nextResolve) {
  const target = targets.get(specifier);
  if (target) return { url: pathToFileURL(target).href, shortCircuit: true };
  return nextResolve(specifier, context);
}

