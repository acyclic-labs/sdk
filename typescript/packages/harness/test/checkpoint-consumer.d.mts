export interface CheckpointFixture {
  readonly authority: { readonly kind: "agent"; readonly id: string };
  readonly issuerId: string;
  readonly issuerKey: Uint8Array;
  readonly schemas?: readonly unknown[];
}
export function exerciseCheckpoint(options: CheckpointFixture): Promise<void>;
