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
    const empty = { rustGrpc: false, rustHttp: false, typescriptGrpcNodeBun: false, typescriptHttp: false, typescriptGrpcWeb: false, typescriptPackageExported: false };
    if (family === "objects" && version === "v1") return empty;
    const objects = family === "objects";
    const stream = family === "stream";
    const unified = family === "actors" || family === "workers";
    const rustBase = `rust/crates/${family}/src/`;
    const tsBase = `typescript/packages/${family}/src/`;
    const operation = objects ? objectMethods[rpc.name] ?? rpc.localName : rpc.localName;
    const rustName = snake(operation);
    const rustRoot = source(`${rustBase}${objects ? "v2/mod.rs" : "lib.rs"}`);
    const grpc = source(`${rustBase}${objects ? "v2/" : ""}grpc.rs`);
    const http = source(`${rustBase}${objects ? "v2/" : ""}http.rs`);
    // Actors' tonic declarations are generated in Cargo OUT_DIR, so the clean
    // checkout has no stable generated .rs path to inspect. The checked-in
    // operation macro, Rust route table, and generated TS facade are the
    // Rust-owned metadata surfaces for this bounded inspector fallback.
    const actorsClient = family === "actors" ? source("rust/crates/actors/src/client.rs") : "";
    const actorsTypescriptClient = family === "actors" ? source("typescript/packages/actors/src/client.ts") : "";
    const facade = unified ? source(`${tsBase}generated/${family}-service.ts`) : "";
    const client = unified ? source(`${tsBase}client.ts`) : "";
    const installer = `install${serviceName.replace(/Service$/u, "")}Methods`;
    const semanticOperation = unified &&
      facade.includes(`"${rpc.input.typeName}":`) && facade.includes(`"${rpc.output.typeName}":`) &&
      facade.includes("readonly [K in keyof Methods]") &&
      facade.includes(`for (const method of ${serviceName}.methods)`) &&
      facade.includes("Object.defineProperty(target, method.localName") && client.includes(`${installer}(this,`);
    const workersBinding = family === "workers" ? source(`${tsBase}binding.ts`) : "";
    const workersClient = family === "workers" ? source(`${rustBase}client_binding.rs`) : "";
    const module = name => new RegExp(`^pub mod ${name};`, "m").test(rustRoot);
    const rustGrpc = module("grpc") && (family === "actors"
      ? new RegExp(`\\boperation!\\(\\s*${rustName}\\s*,`).test(actorsClient)
      : family === "workers"
      ? grpc.includes("pub type Client = wire::workers_service_client::WorkersServiceClient<") &&
        new RegExp(`\\("${rpc.name}",\\s*${rustName},`).test(workersClient) &&
        workersClient.includes("client.$method(request(")
      : objects || stream
      ? rustMethod(rustImpl(grpc, objects ? "impl ObjectsProvider for GrpcObjects" : "impl StreamProvider for Client"), rustName)
      : grpc.includes(`pub type Client = wire::${family}_service_client::${serviceName}Client<`) &&
        rustMethod(source(`${rustBase}generated/acyclic.${family}.${version}.tonic.rs`).split(`pub mod ${family}_service_server`)[0], rustName));
    const rustHttp = module("http") && rustMethod(objects || stream
      ? rustImpl(http, objects ? "impl ObjectsProvider for HttpObjects" : "impl StreamProvider for HttpStream") : http, rustName);
    const grpcPath = `${tsBase}${objects ? "v2-grpc" : "grpc"}.ts`;
    const typescriptGrpcNodeBun = family === "actors"
      ? semanticOperation &&
        actorsTypescriptClient.includes("nativeBinding()") && actorsTypescriptClient.includes("wasmBinding()")
      : family === "workers"
      ? semanticOperation && workersBinding.includes("function loadNative()") &&
        workersBinding.includes("module.WorkersClient.connectResult(config,") &&
        client.includes("export const createWorkersGrpcClient =") && client.includes("=> new WorkersClient(options)") && exported(grpcPath, "./client.js")
      : factory(grpcPath, objects ? "createObjectsV2GrpcClients" : `create${family[0].toUpperCase()}${family.slice(1)}GrpcClient`, serviceName);
    // Unified clients use gRPC-Web in browsers; their compatibility aliases do
    // not expose the retired raw HTTP implementation. Inventory both honestly.
    const typescriptHttp = unified
      ? false
      : objects
      ? method(`${tsBase}v2.ts`, "ObjectsV2Provider", operation) && method(`${tsBase}v2-http.ts`, "HttpObjectsV2", "invoke") && source(`${tsBase}v2-http.ts`).includes("extends ObjectsV2Provider")
      : method(`${tsBase}http.ts`, stream ? "HttpStreamProvider" : `Http${family[0].toUpperCase()}${family.slice(1)}Client`, operation);
    const typescriptGrpcWeb = semanticOperation && (family === "actors"
      ? client.includes("function wasmBinding()") && actorsClient.includes("tonic_web_wasm_client::Client::new_with_options(") &&
        source("rust/crates/actors-wasm/src/lib.rs").includes("pub struct ActorsClient")
      : workersBinding.includes("function loadWasm()") && workersBinding.includes("module.WorkersClient.connect(config,") &&
        workersClient.includes("tonic_web_wasm_client::Client::new_with_options(") &&
        new RegExp(`\\("${rpc.name}",\\s*${rustName},`).test(workersClient));
    const manifestText = source(`typescript/packages/${family}/package.json`);
    const manifest = manifestText ? JSON.parse(manifestText) : {};
    const typescriptPackageExported = family === "actors"
      ? manifest.exports?.["."]?.default === "./dist/index.js" &&
        manifest.exports?.["./client"]?.default === "./dist/client.js" &&
        manifest.exports?.["./proto"]?.default === `./generated/proto/${family}/${version}/${family}_pb.js` &&
        exported(`${tsBase}index.ts`, "./client.js")
      : manifest.exports?.["."]?.default === "./dist/index.js" &&
        manifest.exports?.["./grpc"]?.default === `./dist/${objects ? "v2-grpc" : "grpc"}.js` &&
        manifest.exports?.["./proto"]?.default === `./generated/proto/${family}/${version}/${family}_pb.js` &&
        (objects ? exported(`${tsBase}index.ts`, "./v2.js") && exported(`${tsBase}index.ts`, "./v2-http.js") : exported(`${tsBase}index.ts`, family === "workers" ? "./client.js" : "./http.js"));
    return { rustGrpc, rustHttp, typescriptGrpcNodeBun, typescriptHttp, typescriptGrpcWeb: Boolean(typescriptGrpcWeb), typescriptPackageExported: Boolean(typescriptPackageExported) };
  };
}
