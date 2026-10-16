import { create, fromBinary, toBinary, type MessageInitShape } from "@bufbuild/protobuf";
import { CaveatSchema, FilterSchema, GrantSetSchema, LimitsSchema, PrincipalSchema } from "../generated/proto/auth/v1/auth_pb.js";
import initialize, { Capability as RustCapability, TrustSnapshot as RustTrustSnapshot, type FilterClause } from "../generated/wasm/acyclic_auth_wasm.js";

const wasm = new URL("../generated/wasm/acyclic_auth_wasm_bg.wasm", import.meta.url);
if (wasm.protocol === "file:") {
  const { readFile } = await import("node:fs/promises");
  await initialize({ module_or_path: Uint8Array.from(await readFile(wasm)) });
} else {
  await initialize({ module_or_path: wasm });
}

export interface TrustState {
  readonly environment: string;
  readonly environmentKeys: readonly Uint8Array[];
  readonly account: string;
  readonly issuerKeys: readonly Uint8Array[];
  readonly minimumKeyEpoch: bigint;
  readonly revokedKeyIds: readonly string[];
  readonly revokedBlockIds: readonly Uint8Array[];
}

function joined(values: readonly Uint8Array[]): Uint8Array {
  const bytes = new Uint8Array(values.length * 32);
  for (const [index, value] of values.entries()) {
    if (value.length !== 32) throw new TypeError("public keys and block ids must be 32 bytes");
    bytes.set(value, index * 32);
  }
  return bytes;
}

/** Supply only authenticated current trust state; token certificates are not trust anchors. */
export class TrustSnapshot extends RustTrustSnapshot {
  constructor(state: TrustState) {
    super(state.environment, joined(state.environmentKeys), state.account,
      joined(state.issuerKeys), state.minimumKeyEpoch, [...state.revokedKeyIds], joined(state.revokedBlockIds));
  }
}

/** Holder-only operations, backed by the shared Rust verifier. No mint/root-key API. */
export class Capability {
  #inner: RustCapability | undefined;
  private constructor(inner: RustCapability) { this.#inner = inner; }
  static verify(bytes: Uint8Array, trust: TrustSnapshot, nowUnixSeconds: bigint): Capability {
    return new Capability(RustCapability.verify(bytes, trust, nowUnixSeconds));
  }
  #borrow(): RustCapability {
    if (this.#inner === undefined) throw new Error("capability holder was consumed or freed");
    return this.#inner;
  }
  check(trust: TrustSnapshot, nowUnixSeconds: bigint): void {
    this.#borrow().check(trust, nowUnixSeconds);
  }
  attenuate(caveats: readonly MessageInitShape<typeof CaveatSchema>[]): Capability {
    const secret = crypto.getRandomValues(new Uint8Array(32));
    try {
      const encoded = caveats.map(value => toBinary(CaveatSchema, create(CaveatSchema, value)));
      return new Capability(this.#borrow().attenuate(encoded, secret));
    } finally { secret.fill(0); }
  }
  /** Consumes this holder's append proof. */
  seal(): Capability {
    const inner = this.#borrow();
    this.#inner = undefined;
    return new Capability(inner.seal());
  }
  /** Caller owns these sensitive bytes and should clear them after use. */
  encode(): Uint8Array { return this.#borrow().encode(); }
  blockIds(): Uint8Array[] { return this.#borrow().blockIds(); }
  principal() { return fromBinary(PrincipalSchema, this.#borrow().principal()); }
  grants() { return (this.#borrow().grants() as Uint8Array[]).map(value => fromBinary(GrantSetSchema, value)); }
  limits() { return fromBinary(LimitsSchema, this.#borrow().limits()); }
  filters() {
    const holder = this.#borrow();
    return (holder.filters() as FilterClause[]).map(clause => {
      try {
        return { declaringBlock: clause.declaring_block, subjectId: clause.subject_id,
          lineage: clause.lineage, filter: fromBinary(FilterSchema, clause.filter) };
      } finally { clause.free(); }
    });
  }
  expiresAtUnixSeconds(): bigint { return this.#borrow().expiresAtUnixSeconds(); }
  notBeforeUnixSeconds(): bigint { return this.#borrow().notBeforeUnixSeconds(); }
  free(): void { this.#inner?.free(); this.#inner = undefined; }
}
