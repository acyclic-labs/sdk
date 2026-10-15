import { NativeContracts, jsonToolProjection, type ToolContext, defineTool, policyIdentity, defineRuntimeSchema, type FileRef, descriptorFor } from "../../src/index.js";

export const contracts = await NativeContracts.create();

export const imagePolicy = { native: { intent: { kind: "image", detail: "auto" },
  maximum_bytes: 64, maximum_work: 8, configuration: null } } as const;

export const jsonProjection = await jsonToolProjection({});

export function numberTool(name: string, description: string, handler: (context: ToolContext, input: number) => number | Promise<number>) {
  return defineTool<number, number>({ name, revision: "1", description, inputSchema: { type: "number" }, outputSchema: { type: "number" },
    parseInput: parseNumber, projection: jsonProjection, parseOutput: parseNumber }, handler);
}

export const testModel = { provider: "fixture", name: "fixture", revision: "1", options: {} } as const;

export const durableDigest = "ab".repeat(32);

export const approvalPolicyIdentity = await policyIdentity("approval-policy", "1", new Uint8Array(32).fill(1));

export const denyPolicyIdentity = await policyIdentity("deny-policy", "1", new Uint8Array(32).fill(2));

export const scopedPolicyIdentity = await policyIdentity("scoped-policy", "1", new Uint8Array(32).fill(3));

export const numberSchema = await defineRuntimeSchema("number", { type: "number" }, (value: unknown): number => {
  if (typeof value !== "number") throw new TypeError("invalid result");
  return value;
});

export const parseNumber = numberSchema.parse;

export const parseString = (value: unknown): string => {
  if (typeof value !== "string") throw new TypeError("expected string");
  return value;
};

export const parseNull = (value: unknown): null => {
  if (value !== null) throw new TypeError("expected null");
  return null;
};

export async function mailboxFile(byteLength: number): Promise<FileRef> {
  return {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "project", class: "project", owner: { kind: "project", id: "project" } },
    path: "mailbox/payload.bin",
    version: "generation",
    descriptor: await descriptorFor(new Uint8Array(byteLength), "application/octet-stream"),
    display_name: "payload.bin",
  };
}
