import { expect, test } from "bun:test";
import { decodeClientJson, encodeCanonicalJson, initializeClientQueries } from "../src/client.js";
import { validateContract } from "../generated/wasm/acyclic_harness_wasm.js";
import { MODEL_TEST_LIMITS as limits } from "./support/model-limits.mjs";

await initializeClientQueries();

const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/project-merge-receipt.json", import.meta.url)).text()).trim();
const receipt = () => decodeClientJson(new TextEncoder().encode(fixture));
const admit = (value: unknown) => validateContract("project_merge_receipt", value, undefined);

// Shape admission is separate from a provider's authorized join and publication.
// There is no JavaScript parent controller or opaque plan.
test("Rust and TypeScript share the canonical v2 project merge receipt", () => {
  const original = receipt();
  const admitted = admit(original);
  expect(admitted.child.kind).toBe("conversation");
  expect(admitted.source_project.class).toBe("project");
  expect(admitted.target_project.class).toBe("project");
  expect(admitted.notice.kind).toBe("merge");
  expect(admitted.provider_operation_id).toHaveLength(16);
  expect(admitted.provider_proof.provider).toEqual(admitted.target_project.provider);
  expect(typeof admitted.notice.sequence).toBe("bigint");
  validateContract("conversation_message", admitted.notice, limits);
  expect(encodeCanonicalJson(admitted)).toEqual(encodeCanonicalJson(original));
});

test("merge receipt admission rejects inconsistent provider and generation identities", () => {
  const original = receipt();
  expect(() => admit({ ...original,
    provider_proof: { ...original.provider_proof,
      provider: { ...original.provider_proof.provider, namespace: "foreign" } },
  })).toThrow();
  expect(() => admit({ ...original,
    source_generation: { ...original.source_generation,
      provider: { ...original.source_generation.provider, namespace: "foreign" } },
  })).toThrow();
  expect(() => admit({ ...original, source_project: original.target_project })).toThrow();
  expect(() => admit({ ...original, result_generation: original.expected_target_generation })).toThrow();
  expect(() => admit({ ...original, child: { kind: "agent", id: "child" } })).toThrow();
  expect(() => admit({ ...original, notice: { ...original.notice, kind: "assistant" } })).toThrow();
});

test("merge receipts retain nonempty provider operation identity and portable proof numbers", () => {
  const original = receipt();
  expect(() => admit({ ...original, provider_operation_id: [] })).toThrow();
  expect(() => admit({ ...original, provider_operation_id: [0] })).toThrow();
  // The retired JS brand imposed no 64-byte identity ceiling. Keep the existing
  // accepted input at the actual receipt boundary.
  expect(admit({ ...original, provider_operation_id: Array(65).fill(1) }).provider_operation_id).toHaveLength(65);
  for (const unsafe of [Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1, 9_007_199_254_740_992n]) {
    expect(() => admit({ ...original,
      provider_proof: { ...original.provider_proof, statement: { unsafe } },
    })).toThrow();
  }
});

test("merge proof admission snapshots mutable properties once", () => {
  const original = receipt();
  let proofReads = 0;
  const statement = Object.defineProperty({}, "unsafe", {
    enumerable: true,
    get: () => ++proofReads === 1 ? 1 : Number.POSITIVE_INFINITY,
  });
  const admitted = admit({ ...original,
    provider_proof: { ...original.provider_proof, statement },
  });
  expect(proofReads).toBe(1);
  expect(encodeCanonicalJson(admitted.provider_proof.statement)).toEqual(encodeCanonicalJson({ unsafe: 1 }));
});
