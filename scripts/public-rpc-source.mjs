import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { API, SymbolFlags, TypeFlags, SignatureKind } from "typescript/unstable/sync";
import { SyntaxKind } from "typescript/unstable/ast";
import { isCallExpression, isClassDeclaration, isConstructorDeclaration, isExpressionStatement, isFunctionDeclaration } from "typescript/unstable/ast/is";

const objectMethods = { PutBlob: "put", GetBlob: "get", HeadBlob: "head", DeleteBlob: "delete", ListBlobs: "list" };
// These RPCs compose the same canonical provider operations; they are not
// independent methods on each adapter. Unknown RPCs refuse inventory generation.
const streamMethods = {
  InspectIdempotency: "inspect_idempotency", Append: "write", Fork: "write", Commit: "write",
  Head: "heads", Heads: "heads", Tail: "heads", Children: "heads", ChildrenPage: "heads",
  Read: "read", ReadCommit: "read_commit",
};
const objectRustMethods = { PutBlobs: "put_batch_sealed" };
const snake = name => name.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);

// Rust inventories are emitted from the exact impl or operation macro rustc
// compiles. TypeScript uses the compiler's exported and inherited symbols,
// including generated mapped types, rather than parsing source spellings.
export function createSourceInspector(root, read = path => {
  const url = new URL(path, root);
  return existsSync(url) ? readFileSync(url, "utf8") : "";
}) {
  const files = [
    "actors/client.ts", "actors/index.ts", "stream/client.ts", "stream/http.ts", "stream/grpc.ts", "stream/index.ts",
    "objects/v1.ts", "objects/v1-http.ts", "objects/v1-grpc.ts", "objects/index.ts", "machines/transport.ts",
  ].map(path => `typescript/packages/${path.replace("/", "/src/")}`);
  const directory = fileURLToPath(root);
  const config = resolve(directory, ".rpc-inventory.tsconfig.json");
  const api = new API({ cwd: directory, fs: { readFile(path) {
    if (resolve(path) === config) return JSON.stringify({ compilerOptions: {
      target: "esnext", module: "nodenext", moduleResolution: "nodenext", skipLibCheck: true, noEmit: true, allowJs: true,
    }, files });
    const relative = path.replaceAll("\\", "/").slice(directory.replaceAll("\\", "/").length);
    return relative && !relative.startsWith("..") && resolve(directory, relative) === resolve(path)
      ? read(relative.replace(/^\//, "")) || null : undefined;
  } } });
  const snapshot = api.updateSnapshot({ openProjects: [config] });
  const project = snapshot.getProject(config);
  if (!project) {
    snapshot.dispose(); api.close();
    throw new Error("RPC compiler inventory could not open its project; recovery: bun run test:contracts");
  }
  const checker = project.checker;
  const module = path => project.program.getSourceFile(resolve(directory, `typescript/packages/${path}`));
  const exported = (path, name) => {
    const file = module(path);
    const symbol = file && checker.getSymbolAtLocation(file);
    return symbol && checker.getExportsOfModule(symbol).find(symbol => symbol.name === name);
  };
  const original = symbol => symbol && (symbol.flags & SymbolFlags.Alias ? checker.getAliasedSymbol(symbol) : symbol);
  const value = symbol => symbol && checker.getTypeOfSymbol(original(symbol));
  const property = (type, name) => type && value(checker.getPropertyOfType(type, name));
  const callable = type => Boolean(type && !(type.flags & (TypeFlags.Any | TypeFlags.Unknown)) && checker.getSignaturesOfType(type, SignatureKind.Call).length);
  const member = (path, owner, name) => {
    const symbol = original(exported(path, owner));
    return Boolean(symbol && callable(property(checker.getDeclaredTypeOfSymbol(symbol), name)));
  };
  const factory = (path, name, field, operation) => {
    const type = value(exported(path, name));
    if (!callable(type)) return false;
    let result = checker.getReturnTypeOfSignature(checker.getSignaturesOfType(type, SignatureKind.Call)[0]);
    if (field) result = property(result, field);
    return callable(property(result, operation));
  };
  const binding = name => {
    const file = module("actors/src/client.ts");
    return Boolean(file && file.statements.some(statement => isFunctionDeclaration(statement) && statement.name?.text === name && callable(value(checker.getSymbolAtLocation(statement.name)))));
  };
  const actorsInstalled = () => {
    const installer = original(exported("actors/src/generated/actors-service.ts", "installActorsMethods"));
    if (!installer || !callable(value(installer))) return false;
    const client = module("actors/src/client.ts")?.statements.find(statement =>
      isClassDeclaration(statement) && statement.name?.text === "ActorsClient");
    const constructor = client?.members.find(isConstructorDeclaration);
    return Boolean(constructor?.body?.statements.some(statement => {
      if (!isExpressionStatement(statement) || !isCallExpression(statement.expression)) return false;
      const call = statement.expression;
      return call.arguments[0]?.kind === SyntaxKind.ThisKeyword &&
        original(checker.getSymbolAtLocation(call.expression))?.id === installer.id &&
        Boolean(checker.getResolvedSignature(call));
    }));
  };
  const installed = actorsInstalled();
  const inventories = new Map();
  const inventory = family => {
    if (!inventories.has(family)) {
      const text = read(`compatibility/rpc-implementations/${family}.json`);
      if (!text) throw new Error(`RPC inventory missing for ${family}; recovery: bun scripts/generate-contracts.mjs write ${family}`);
      inventories.set(family, JSON.parse(text));
    }
    return inventories.get(family);
  };

  const inspect = (service, rpc) => {
    const [, family, version] = service.typeName.split(".");
    if ((family === "objects" || family === "stream") && version !== "v1") throw new Error("unsupported current contract");
    const objects = family === "objects";
    const stream = family === "stream";
    const actors = family === "actors";
    const operation = objects ? objectMethods[rpc.name] ?? rpc.localName : rpc.localName;
    const rustName = stream ? streamMethods[rpc.name] : objects ? objectRustMethods[rpc.name] ?? snake(operation) : snake(operation);
    if (stream && !rustName) throw new Error(`RPC provider mapping missing for ${rpc.name}; recovery: update the canonical provider composition in scripts/public-rpc-source.mjs`);
    const rust = actors || objects || stream ? inventory(family) : { grpc: [], http: [] };
    const typescriptGrpcNodeBun = actors
      ? installed && member("actors/src/client.ts", "ActorsClient", operation) && binding("nativeBinding")
      : objects ? factory("objects/src/v1-grpc.ts", "createObjectsV1GrpcClients", "blobs", rpc.localName)
      : stream && factory("stream/src/grpc.ts", "createStreamGrpcClient", undefined, rpc.localName);
    const typescriptHttp = objects
      ? member("objects/src/v1-http.ts", "HttpObjectsV1", operation) && member("objects/src/v1-http.ts", "HttpObjectsV1", "open")
      : stream && (rpc.name === "Children"
        ? member("stream/src/client.ts", "StreamClient", "children") && member("stream/src/http.ts", "HttpStreamProvider", "childrenPage")
        : member("stream/src/http.ts", "HttpStreamProvider", operation));
    const manifestText = read(`typescript/packages/${family}/package.json`);
    const manifest = manifestText ? JSON.parse(manifestText) : {};
    const typescriptPackageExported = manifest.exports?.["."]?.default === "./dist/index.js" &&
      (actors ? manifest.exports?.["./client"]?.default === "./dist/client.js" && Boolean(exported("actors/src/index.ts", "ActorsClient"))
      : objects ? manifest.exports?.["./grpc"]?.default === "./dist/v1-grpc.js" && Boolean(exported("objects/src/index.ts", "HttpObjectsV1")) && Boolean(exported("objects/src/index.ts", "ObjectsV1Provider"))
      : stream ? manifest.exports?.["./grpc"]?.default === "./dist/grpc.js" && Boolean(exported("stream/src/index.ts", "HttpStreamProvider"))
      : family === "machines" && manifest.exports?.["./transport"]?.default === "./dist/transport.js" && factory("machines/src/transport.ts", "createMachinesClient", "machines", rpc.localName)) &&
      manifest.exports?.["./proto"]?.default === `./generated/proto/${family}/${version}/${family}_pb.js`;
    return {
      rustGrpc: rust.grpc.includes(rustName), rustHttp: rust.http.includes(rustName),
      typescriptGrpcNodeBun: Boolean(typescriptGrpcNodeBun), typescriptHttp: Boolean(typescriptHttp),
      typescriptGrpcWeb: actors && installed && rust.wasm?.includes(rustName) && member("actors/src/client.ts", "ActorsClient", operation) && binding("wasmBinding"),
      typescriptPackageExported: Boolean(typescriptPackageExported),
    };
  };
  inspect.routes = family => inventory(family).routes;
  inspect.close = () => { snapshot.dispose(); api.close(); };
  return inspect;
}
