import { existsSync, readFileSync } from "node:fs";

// Ignore comments and literals when locating declaration bodies. Preserve offsets
// so nested braces can be balanced without confusing request objects with methods.
const codeOnly = text => text.replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|`(?:\\.|[^`\\])*`/g,
  match => match.replace(/[^\n]/g, " "));
function body(source, declaration) {
  const code = codeOnly(source);
  const match = declaration.exec(code);
  if (!match) return "";
  const start = code.indexOf("{", match.index + match[0].length);
  if (start < 0) return "";
  let depth = 1;
  for (let end = start + 1; end < code.length; end++) {
    if (code[end] === "{") depth++;
    if (code[end] === "}" && --depth === 0) return code.slice(start + 1, end);
  }
  return "";
}

const objectMethods = {
  PutObject: "put", GetObject: "get", HeadObject: "head", DeleteObject: "delete", ListObjects: "list",
};
const snake = name => name.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
const rustMethod = (source, name) => new RegExp(`^\\s*(?:pub\\s+)?async\\s+fn\\s+${name}\\s*\\(`, "m").test(source);
const rustImpl = (source, header) => source.match(new RegExp(`${header}\\s*\\{([\\s\\S]*?)^\\}`, "m"))?.[1] ?? "";

// These checks establish source exposure. Runtime semantics are proved separately
// by the complete transport fixtures, not by the inventory's presence flags.
export function createSourceInspector(root, read = path => {
  const url = new URL(path, root);
  return existsSync(url) ? readFileSync(url, "utf8") : "";
}) {
  const sources = new Map();
  const source = path => {
    if (!sources.has(path)) sources.set(path, read(path));
    return sources.get(path);
  };
  const method = (path, className, name) => new RegExp(`^\\s*(?:(?:public|protected|private|async)\\s+)*\\*?${name}\\s*\\(`, "m").test(
    body(source(path), new RegExp(`\\bclass\\s+${className}\\b`)));
  const factory = (path, name, service) => {
    const implementation = body(source(path), new RegExp(`\\bexport\\s+function\\s+${name}\\s*\\(`));
    return new RegExp(`\\bcreateClient\\s*\\(\\s*${service}\\s*,`).test(implementation) &&
      /\bcreateGrpcTransport\s*\(/.test(implementation);
  };
  const exported = (path, module) => new RegExp(`export\\s+\\*\\s+from\\s+["']${module.replaceAll(".", "\\.")}["']`).test(source(path));

  return (service, rpc) => {
    const [, family, version, serviceName] = service.typeName.split(".");
    const objects = family === "objects";
    const stream = family === "stream";
    if ((objects || stream) && version !== "v1") throw new Error("unsupported current contract");
    const rustBase = `rust/crates/${family}/src/`;
    const tsBase = `typescript/packages/${family}/src/`;
    const operation = objects ? objectMethods[rpc.name] ?? rpc.localName : rpc.localName;
    const rustName = snake(operation);
    if (objects) {
      const contracts = source(`${rustBase}lib.rs`).match(/^pub mod v[0-9]+;/gm) ?? [];
      if (contracts.length !== 1 || contracts[0] !== `pub mod ${version};`) throw new Error("Objects must expose one current contract module");
    }
    const rustRoot = source(`${rustBase}${objects ? "v1/mod.rs" : "lib.rs"}`);
    const grpc = source(`${rustBase}${objects ? "v1/" : ""}grpc.rs`);
    const http = source(`${rustBase}${objects ? "v1/" : ""}http.rs`);
    const module = name => new RegExp(`^pub mod ${name};`, "m").test(rustRoot);
    const rustGrpc = module("grpc") && (objects || stream
      ? rustMethod(rustImpl(grpc, objects ? "impl ObjectsProvider for GrpcObjects" : "impl StreamProvider for Client"), rustName)
      : grpc.includes(`pub type Client = wire::${family}_service_client::${serviceName}Client<`) &&
        rustMethod(source(`${rustBase}generated/acyclic.${family}.${version}.tonic.rs`).split(`pub mod ${family}_service_server`)[0], rustName));
    const rustHttp = module("http") && rustMethod(objects || stream
      ? rustImpl(http, objects ? "impl ObjectsProvider for HttpObjects" : "impl StreamProvider for HttpStream") : http, rustName);
    const grpcPath = `${tsBase}${objects ? "v1-grpc" : "grpc"}.ts`;
    const typescriptGrpcNodeBun = factory(grpcPath, objects ? "createObjectsV1GrpcClients" : `create${family[0].toUpperCase()}${family.slice(1)}GrpcClient`, serviceName);
    const typescriptHttp = objects
      ? method(`${tsBase}v1.ts`, "ObjectsV1Provider", operation) && method(`${tsBase}v1-http.ts`, "HttpObjectsV1", "invoke") && source(`${tsBase}v1-http.ts`).includes("extends ObjectsV1Provider")
      : method(`${tsBase}http.ts`, stream ? "HttpStreamProvider" : `Http${family[0].toUpperCase()}${family.slice(1)}Client`, operation);
    const manifestText = source(`typescript/packages/${family}/package.json`);
    const manifest = manifestText ? JSON.parse(manifestText) : {};
    const typescriptPackageExported = manifest.exports?.["."]?.default === "./dist/index.js" &&
      manifest.exports?.["./grpc"]?.default === `./dist/${objects ? "v1-grpc" : "grpc"}.js` &&
      manifest.exports?.["./proto"]?.default === `./generated/proto/${family}/${version}/${family}_pb.js` &&
      (objects ? exported(`${tsBase}index.ts`, "./v1.js") && exported(`${tsBase}index.ts`, "./v1-http.js") : exported(`${tsBase}index.ts`, "./http.js"));
    return { rustGrpc, rustHttp, typescriptGrpcNodeBun, typescriptHttp, typescriptPackageExported: Boolean(typescriptPackageExported) };
  };
}
