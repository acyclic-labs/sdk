/** A member's original status. Diagnostic only; recovery follows the product's
 * semantic code. Metadata values retain their original header bytes. */
export interface Refusal {
  readonly status: number;
  readonly message: string;
  readonly details: Uint8Array;
  readonly metadata: ReadonlyArray<readonly [string, Uint8Array]>;
}
