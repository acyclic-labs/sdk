import { join } from "node:path";
import { pathToFileURL } from "node:url";

const root = process.env.ACYCLIC_SDK_ROOT;
if (!root) throw new Error("ACYCLIC_SDK_ROOT must name the SDK checkout");
const dependencyRoot = process.env.ACYCLIC_NODE_MODULE_ROOT ?? root;
const bun = join(dependencyRoot, "node_modules", ".bun");
const dep = process.env.ACYCLIC_DEP_ROOT;
const packagePath = (bunName, scope, name) => dep
  ? join(dep, scope, name)
  : join(bun, bunName, "node_modules", scope, name);
const targets = new Map([
  ["@bufbuild/protobuf", join(packagePath("@bufbuild+protobuf@2.15.0", "@bufbuild", "protobuf"), "dist", "esm", "index.js")],
  ["@connectrpc/connect", join(packagePath("@connectrpc+connect@2.1.1+432d72b9ceda49ac", "@connectrpc", "connect"), "dist", "esm", "index.js")],
  ["@connectrpc/connect-node", join(packagePath("@connectrpc+connect-node@2.1.1+362e60f3a5b2a079", "@connectrpc", "connect-node"), "dist", "esm", "index.js")],
]);

export function resolve(specifier, context, nextResolve) {
  const target = targets.get(specifier);
  if (target) return { url: pathToFileURL(target).href, shortCircuit: true };
  if (specifier.startsWith("@connectrpc/connect/")) {
    const subpath = specifier.slice("@connectrpc/connect/".length);
    const leaf = ["protocol", "protocol-connect", "protocol-grpc", "protocol-grpc-web"].includes(subpath)
      ? join(subpath, "index.js")
      : `${subpath}.js`;
    return { url: pathToFileURL(join(packagePath("@connectrpc+connect@2.1.1+432d72b9ceda49ac", "@connectrpc", "connect"), "dist", "esm", leaf)).href, shortCircuit: true };
  }
  if (specifier.startsWith("@bufbuild/protobuf/")) {
    const subpath = specifier.slice("@bufbuild/protobuf/".length);
    const leaf = ["wkt", "wire", "reflect", "codegenv1", "codegenv2", "txtpb"].includes(subpath)
      ? join(subpath, "index.js")
      : `${subpath}.js`;
    return { url: pathToFileURL(join(packagePath("@bufbuild+protobuf@2.15.0", "@bufbuild", "protobuf"), "dist", "esm", leaf)).href, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}

