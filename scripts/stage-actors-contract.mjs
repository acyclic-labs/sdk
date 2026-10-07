// The Actors protobuf is rendered from the Rust Protify contract before any
// Buf generation runs. Keeping this as a normal contract generator module
// means the shared generator build compiles the Rust renderer once and the
// resulting bytes are the sole input to both TypeScript and descriptor work.
export const rust = [["acyclic-actors", "example", "actors-contract"]];

export function render([proto]) {
  if (typeof proto !== "string" || !proto.includes("syntax = \"proto3\";")) {
    throw new Error("Actors Rust contract renderer did not emit a proto3 schema");
  }
  return { "proto/actors/v1/actors.proto": proto.endsWith("\n") ? proto : `${proto}\n` };
}