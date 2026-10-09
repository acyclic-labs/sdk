import { DEFAULT_LIMITS } from "../../src/conversation.js";

/** This synthetic provider declares canonical UTF-8 bytes and file body bytes
 * as token units, with 512 units of framing per request and message. Rust owns
 * the complete nested media inventory, including immutable native options. */
/** @param {import("../../src/native-contracts.js").NativeContracts} contracts
 * @returns {Pick<import("../../src/model.js").ModelProvider, "contextCapacity" | "countTokens">} */
export function syntheticAccounting(contracts) {
  return {
    contextCapacity() { return { contextTokens: 1_048_576, outputTokens: 16_384 }; },
    countTokens(request) {
      return {
        requestDigest: contracts.digestCanonicalJson(contracts.decodeCanonicalJson(request.serializedInput)),
        fixedTokens: request.serializedInput.byteLength + 512,
        messageTokens: request.messages.map(message => {
          const bytes = 512 + contracts.modelContentInventory(message.content, DEFAULT_LIMITS).files
            .reduce((total, file) => total + file.descriptor.byte_length, 0);
          if (!Number.isSafeInteger(bytes) || bytes > 0xffff_ffff) throw new RangeError("synthetic message count exceeds u32");
          return bytes;
        }),
      };
    },
  };
}
