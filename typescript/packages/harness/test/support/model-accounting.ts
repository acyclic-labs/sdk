import type { NativeContracts } from "../../src/native-contracts.js";
import type { ModelContent, ModelContentPart, ModelProvider } from "../../src/model.js";

/** This synthetic provider declares UTF-8 bytes as token units, including file
 * bodies. Counting the complete canonical request as fixed framing deliberately
 * overcounts message descriptors, so schemas/options are never omitted. */
export function syntheticAccounting(contracts: NativeContracts): Pick<ModelProvider, "contextCapacity" | "countTokens"> {
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

function fileBytes(content: ModelContent): number {
  const parts: readonly ModelContentPart[] = typeof content === "string" ? []
    : "kind" in content ? [content] : content;
  return parts.reduce((bytes, part) => bytes + (part.kind === "file" ? part.file.descriptor.byte_length : 0), 0);
}
