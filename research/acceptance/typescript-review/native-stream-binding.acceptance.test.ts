import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../../../", import.meta.url));

async function source(relative: string): Promise<string> {
  return readFile(join(root, relative), "utf8");
}

test("the existing embedded C ABI is memory-only and cannot configure remote gRPC", async () => {
  const [manifest, embedded] = await Promise.all([
    source("rust/crates/sdk-embedded-prototype/Cargo.toml"),
    source("rust/crates/sdk-embedded-prototype/src/lib.rs"),
  ]);

  expect(manifest).toContain("Bounded C ABI prototype over the Rust Stream memory provider");
  expect(embedded).toContain("provider: Arc<MemoryStream>");
  expect(embedded).toContain("provider: Arc::new(MemoryStream::new(MemoryLimits::default()))");
  expect(embedded).toContain("acyclic_embedded_engine_open");
  expect(embedded).not.toContain("grpc::Client");
  expect(embedded).not.toContain("connect_endpoints");
  expect(embedded).not.toContain("certificate_pem");
  expect(embedded).not.toContain("MAX_ENDPOINTS");
  expect(embedded).not.toContain("endpoint pool");
});

test("the current WASM and UniFFI surfaces also bind local memory only", async () => {
  const [wasm, uniffi] = await Promise.all([
    source("rust/crates/stream-wasm/src/lib.rs"),
    source("rust/crates/sdk-embedded-prototype/src/uniffi_polling.rs"),
  ]);

  expect(wasm).toContain("pub struct MemoryStreamBinding");
  expect(wasm).toContain("inner: MemoryStream");
  expect(wasm).toContain("pub fn new() -> Self");
  expect(wasm).not.toContain("grpc::Client");
  expect(wasm).not.toContain("connect_endpoints");
  expect(wasm).not.toContain("certificate_pem");
  expect(uniffi).toContain("canonical `MemoryStream` provider");
  expect(uniffi).toContain("provider: Arc::new(acyclic_stream::MemoryStream::new");
  expect(uniffi).not.toContain("grpc::Client");
  expect(uniffi).not.toContain("connect_endpoints");
  expect(uniffi).not.toContain("certificate_pem");
});

test("canonical Rust gRPC exposes the endpoint pool and bounded recovery contract", async () => {
  const grpc = await source("rust/crates/stream/src/grpc.rs");

  expect(grpc).toContain("pub const MAX_ENDPOINTS: usize = 16");
  expect(grpc).toContain("pub const MAX_ENDPOINT_URI_BYTES: usize = 2_048");
  expect(grpc).toContain("pub const MAX_CA_CERTIFICATE_BYTES: usize = 64 * 1024");
  expect(grpc).toContain("const OPERATION_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10)");
  expect(grpc).toContain("const OPERATION_ENDPOINT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1)");
  expect(grpc).toContain("const FOLLOW_ENDPOINT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500)");
  expect(grpc).toContain("const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(10)");
  expect(grpc).toContain("pub async fn connect_endpoints<I, S>");
  expect(grpc).toContain("pub async fn connect_endpoints_with_ca_certificate<I, S>");
  expect(grpc).toContain("fn retryable");
  expect(grpc).toContain("advance_follow");
  expect(grpc).toContain("preferred: Arc<AtomicUsize>");
});

test("the TypeScript native adapter currently creates one Connect transport", async () => {
  const [grpc, packageJson] = await Promise.all([
    source("typescript/packages/stream/src/grpc.ts"),
    source("typescript/packages/stream/package.json"),
  ]);

  expect(grpc).toContain("createGrpcTransport");
  expect(grpc).toContain("baseUrl: endpoint.href");
  expect(grpc).toContain("nodeOptions: { ca: [...rootCertificates, options.caCertificate] }");
  expect(grpc).not.toContain("connect_endpoints");
  expect(grpc).not.toContain("MAX_ENDPOINTS");
  expect(grpc).not.toContain("OPERATION_DEADLINE");
  expect(grpc).not.toContain("advance_follow");
  expect(grpc).not.toContain("retryable");
  expect(packageJson).toContain('"@connectrpc/connect-node": "2.1.1"');
  expect(packageJson).not.toContain("acyclic-stream-napi");
  expect(packageJson).not.toContain("optionalDependencies");
});
