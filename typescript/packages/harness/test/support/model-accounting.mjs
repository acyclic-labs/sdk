/** This synthetic provider declares UTF-8 bytes as token units, including file
 * bodies. Counting the complete canonical request as fixed framing deliberately
 * overcounts message descriptors, so schemas/options are never omitted. */
/** @param {import("../../src/native-contracts.js").NativeContracts} contracts
 * @returns {Pick<import("../../src/model.js").ModelProvider, "contextCapacity" | "countTokens">} */
export function syntheticAccounting(contracts) {
  return {
    contextCapacity() { return { contextTokens: 1_048_576, outputTokens: 16_384 }; },
    countTokens(request) {
      return {
        requestDigest: contracts.digestCanonicalJson(contracts.decodeModelJson(request.serializedInput)),
        fixedTokens: request.serializedInput.byteLength + 512,
        messageTokens: request.messages.map(message => {
          const bytes = new TextEncoder().encode(JSON.stringify(message)).byteLength + fileBytes(message.content);
          if (!Number.isSafeInteger(bytes) || bytes > 0xffff_ffff) throw new RangeError("synthetic message count exceeds u32");
          return bytes;
        }),
      };
    },
  };
}

/** @param {import("../../src/model.js").ModelContent} content */
function fileBytes(content) {
  const parts = typeof content === "string" ? []
    : "kind" in content ? [content] : content;
  return parts.reduce((bytes, part) => bytes + (part.kind === "file" ? part.file.descriptor.byte_length : 0), 0);
}
