import type { CanonicalCompatible, ReadonlyBytes, ReadonlyInputSemantic, ReadonlySemantic } from "./readonly.js";

declare const brand: unique symbol;
type Identifier = string & { readonly [brand]: "Identifier" };
type Digest = Uint8Array & { readonly [brand]: "Digest" };
type Model = { id: Identifier; bytes: Digest; nested: { values: bigint[] }[]; optional?: { bytes: Uint8Array } };
declare const output: ReadonlySemantic<Model>;
declare const identifier: Identifier;
declare const digest: Digest;

const input: ReadonlyInputSemantic<Model> = { id: identifier, bytes: digest, nested: [{ values: [1n] }] };
const brandedBytes: ReadonlyBytes<Digest> = output.bytes;
void [input, brandedBytes];

// @ts-expect-error A plain string cannot supply a Rust nominal identifier.
const unbranded: ReadonlyInputSemantic<Model> = { id: "plain", bytes: digest, nested: [] };
// @ts-expect-error Projected byte indices cannot be assigned.
output.bytes[0] = 1;
// @ts-expect-error Mutating typed-array methods are absent.
output.bytes.fill(0);
// @ts-expect-error Returned byte views stay readonly.
output.bytes.subarray(0)[0] = 1;
// @ts-expect-error Byte callback arguments stay readonly.
output.bytes.forEach((_value, _index, bytes) => bytes[0] = 1);
// @ts-expect-error Nested scalar collections stay readonly.
output.nested[0].values.push(2n);
// @ts-expect-error Optional nested bytes stay readonly.
if (output.optional) output.optional.bytes[0] = 1;

type Wire = { $typeName: "fixture.Model"; id: string; bytes: Uint8Array; nested: { values: bigint[] }[]; optional?: { bytes: Uint8Array } };
const compatible: CanonicalCompatible<ReadonlySemantic<Model>, Wire> = true;
// @ts-expect-error Replacing bigint with number changes the semantic scalar contract.
const wrongScalar: CanonicalCompatible<{ id: Identifier; bytes: Digest; nested: { values: number[] }[]; optional?: { bytes: Uint8Array } }, Wire> = true;
// @ts-expect-error Omitting a field changes the canonical shape.
const missingField: CanonicalCompatible<Omit<Model, "nested">, Wire> = true;
// @ts-expect-error An optional semantic field cannot represent required wire presence.
const wrongPresence: CanonicalCompatible<{ id?: Identifier }, { id: string }> = true;

type SemanticChoice = { choice: { case: "value"; value: Identifier } | { case: undefined } };
type WireChoice = { choice: { case: "value"; value: string } | { case: undefined } };
const compatibleChoice: CanonicalCompatible<SemanticChoice, WireChoice> = true;
// @ts-expect-error An unmatched discriminator changes the protobuf oneof contract.
const wrongChoice: CanonicalCompatible<{ choice: { case: "other"; value: Identifier } }, WireChoice> = true;
void [unbranded, compatible, wrongScalar, missingField, wrongPresence, compatibleChoice, wrongChoice];
