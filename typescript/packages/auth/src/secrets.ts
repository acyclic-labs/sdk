import { create, fromBinary, toBinary, type DescMessage, type MessageInitShape } from "@bufbuild/protobuf";
import { SecretPutRequestSchema, SecretListRequestSchema, SecretDeleteRequestSchema,
  SecretMutationReplySchema, SecretListReplySchema } from "../generated/proto/auth/v1/auth_pb.js";
import { SecretsClient as RustSecretsClient } from "./wasm.js";

// Inspect the generated descriptor rather than maintaining another request
// schema. Protobuf create() would silently discard an account selector.
function admit<Schema extends DescMessage>(schema: Schema, request: MessageInitShape<Schema>) {
  try {
    const fields = new Set(schema.fields.map(field => field.localName));
    if (Object.keys(request).some(key => !fields.has(key)
      && !(key === "$typeName" && Reflect.get(request, key) === schema.typeName))) throw new Error();
    return toBinary(schema, create(schema, request));
  } catch {
    // Generated codecs may report an invalid input's actual contents.
    throw new Error("secrets: invalid request");
  }
}

/** Shared Rust client. Generation/version fields stay bigint at the JS boundary. */
export class SecretsClient {
  readonly #inner: RustSecretsClient;
  constructor(endpoint: string, credential: () => string | Promise<string>) {
    this.#inner = new RustSecretsClient(endpoint, credential);
  }
  async put(request: MessageInitShape<typeof SecretPutRequestSchema>) {
    const bytes = admit(SecretPutRequestSchema, request);
    try { return fromBinary(SecretMutationReplySchema, await this.#inner.put(bytes)); }
    finally { bytes.fill(0); }
  }
  async list(request: MessageInitShape<typeof SecretListRequestSchema>) {
    const bytes = admit(SecretListRequestSchema, request);
    return fromBinary(SecretListReplySchema, await this.#inner.list(bytes));
  }
  async delete(request: MessageInitShape<typeof SecretDeleteRequestSchema>) {
    const bytes = admit(SecretDeleteRequestSchema, request);
    return fromBinary(SecretMutationReplySchema, await this.#inner.delete(bytes));
  }
  free(): void { this.#inner.free(); }
}
