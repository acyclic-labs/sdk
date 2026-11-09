import type { VerifyHmacRequest } from "./contract.js";
import { snapshotBytes } from "./owned-bytes.js";

/** Original invocation's protected host port. Never expose this port to guests.
 * Rust owns the request contract; hosts resolve only admitted versioned selections.
 * Unknown bindings and unavailable secrets reject with fixed, redacted errors.
 */
export interface SecretBindingHost {
  verifyHmac(request: VerifyHmacRequest): Promise<boolean>;
}

/** Guest surface contains only verification. No get, sign, export or resolver. */
export interface SecretBinding {
  verifyHmac(alg: VerifyHmacRequest["alg"], body: VerifyHmacRequest["body"], signature: VerifyHmacRequest["signature"]): Promise<boolean>;
}

/** Retain only the original host operation and receiver, never any host fields. */
export function materializeSecretBinding(name: string, host: SecretBindingHost): SecretBinding {
  const verify = host.verifyHmac.bind(host);
  return Object.freeze({
    async verifyHmac(alg: string, body: Uint8Array, signature: Uint8Array): Promise<boolean> {
      try {
        const result = await verify({ secretBinding: name, alg, body: snapshotBytes(body), signature: snapshotBytes(signature) });
        if (typeof result !== "boolean") throw new Error();
        return result;
      } catch {
        // A provider error or malformed reply must never disclose provider data.
        throw new Error("secret binding verification unavailable");
      }
    },
  });
}

/** Verify raw bytes; parse provider-specific signature encodings in the app. */
export function verifyHmac(secretBinding: SecretBinding, alg: string, body: Uint8Array, signature: Uint8Array): Promise<boolean> {
  return secretBinding.verifyHmac(alg, body, signature);
}
