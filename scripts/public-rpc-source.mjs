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
    const empty = { rustGrpc: false, rustHttp: false, typescriptGrpcNodeBun: false, typescriptHttp: false, typescriptPackageExported: false };
    if (family === "objects" && version === "v1") return empty;
    const objects = family === "objects";
    const stream = family === "stream";
    const deliveryAck = stream && rpc.name === "AcknowledgeDelivery";
    const rustBase = `rust/crates/${family}/src/`;
    const tsBase = `typescript/packages/${family}/src/`;
    const operation = objects ? objectMethods[rpc.name] ?? rpc.localName : rpc.localName;
    const rustName = snake(operation);
    const rustRoot = source(`${rustBase}${objects ? "v2/mod.rs" : "lib.rs"}`);
    const grpc = source(`${rustBase}${objects ? "v2/" : ""}grpc.rs`);
    const http = source(`${rustBase}${objects ? "v2/" : ""}http.rs`);
    const module = name => new RegExp(`^pub mod ${name};`, "m").test(rustRoot);
    const rustGrpc = module("grpc") && (objects || stream
      ? rustMethod(rustImpl(grpc, objects ? "impl ObjectsProvider for GrpcObjects" : deliveryAck ? "impl Client" : "impl StreamProvider for Client"), rustName)
      : grpc.includes(`pub type Client = wire::${family}_service_client::${serviceName}Client<`) &&
        rustMethod(source(`${rustBase}generated/acyclic.${family}.${version}.tonic.rs`).split(`pub mod ${family}_service_server`)[0], rustName));
    const rustHttp = module("http") && rustMethod(objects || stream
      ? rustImpl(http, objects ? "impl ObjectsProvider for HttpObjects" : deliveryAck ? "impl HttpStream" : "impl StreamProvider for HttpStream") : http, rustName);
    const grpcPath = `${tsBase}${objects ? "v2-grpc" : "grpc"}.ts`;
    const typescriptGrpcNodeBun = factory(grpcPath, objects ? "createObjectsV2GrpcClients" : `create${family[0].toUpperCase()}${family.slice(1)}GrpcClient`, serviceName);
    const typescriptHttp = objects
      ? method(`${tsBase}v2.ts`, "ObjectsV2Provider", operation) && method(`${tsBase}v2-http.ts`, "HttpObjectsV2", "invoke") && source(`${tsBase}v2-http.ts`).includes("extends ObjectsV2Provider")
      : method(`${tsBase}http.ts`, stream ? "HttpStreamProvider" : `Http${family[0].toUpperCase()}${family.slice(1)}Client`, operation);
    const manifestText = source(`typescript/packages/${family}/package.json`);
    const manifest = manifestText ? JSON.parse(manifestText) : {};
    const typescriptPackageExported = manifest.exports?.["."]?.default === "./dist/index.js" &&
      manifest.exports?.["./grpc"]?.default === `./dist/${objects ? "v2-grpc" : "grpc"}.js` &&
      manifest.exports?.["./proto"]?.default === `./generated/proto/${family}/${version}/${family}_pb.js` &&
      (objects ? exported(`${tsBase}index.ts`, "./v2.js") && exported(`${tsBase}index.ts`, "./v2-http.js") : exported(`${tsBase}index.ts`, "./http.js"));
    return { rustGrpc, rustHttp, typescriptGrpcNodeBun, typescriptHttp, typescriptPackageExported: Boolean(typescriptPackageExported) };
  };
}
