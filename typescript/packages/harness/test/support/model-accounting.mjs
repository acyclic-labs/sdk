import { DEFAULT_LIMITS } from "../../dist/index.js";

/** This synthetic provider declares canonical UTF-8 bytes and file body bytes
 * as token units, with 512 units of framing per request and message. Rust owns
 * the complete nested media inventory, including immutable native options. */
/** @param {import("../../src/native-contracts.js").NativeContracts} contracts
 * @returns {{contextCapacity: import("../../src/model.js").ModelProvider["contextCapacity"], countTokens(request: import("../../src/model.js").ModelRequest & {serializedInput: Uint8Array}): Promise<import("../../src/model.js").ModelTokenCount>}} */
export function syntheticAccounting(contracts) {
  return {
    contextCapacity() { return { contextTokens: 1_048_576, outputTokens: 16_384 }; },
    async countTokens(request) {
      const messageTokens = request.messages.map(message => {
        const bytes = 512 + contracts.modelContentInventory(message.content, DEFAULT_LIMITS).files
          .reduce((total, file) => total + file.descriptor.byte_length, 0);
        if (!Number.isSafeInteger(bytes) || bytes > 0xffff_ffff) throw new RangeError("synthetic message count exceeds u32");
        return bytes;
      });
      const fixedTokens = request.serializedInput.byteLength + 512;
      return {
        requestDigest: contracts.digestCanonicalJson(contracts.decodeCanonicalJson(request.serializedInput)),
        inputTokens: BigInt(fixedTokens) + messageTokens.reduce((sum, n) => sum + BigInt(n), 0n),
        fixedTokens, messageTokens,
      };
    },
  };
}

/** @param {import("../../src/native-contracts.js").NativeContracts} contracts
 * @param {(request: import("../../src/model.js").ModelRequest & {serializedInput: Uint8Array}, dispatch: import("../../src/model.js").ModelGeneration) => AsyncIterable<import("../../src/model.js").ModelEvent>} generate
 * @returns {import("../../src/model.js").ModelProvider["prepare"]} */
export function scriptedPreparation(contracts, generate) {
  return async request => ({
    [Symbol.dispose]() {},
    countTokens: () => syntheticAccounting(contracts).countTokens(request),
    generate: dispatch => generate(request, dispatch),
  });
}
