// Generated from Rust scenario inference/typescript-consumer.
// Rust output SHA256: sha256:a95d223479366e9bd1eb04ddc6864f113a3a27e6d7beea05300037cc2351903b
import { create } from "@bufbuild/protobuf";
import { RequestIdentitySchema } from "@acyclic-labs/inference/proto";

const identity = create(RequestIdentitySchema);
if (identity.requestId.byteLength !== 0) throw new Error("generated identity default changed");
const maximumMessageBytes = 8388608;
const maximumHttpJsonBytes = 16777216;
console.log(JSON.stringify({ maximumMessageBytes, maximumHttpJsonBytes }));
