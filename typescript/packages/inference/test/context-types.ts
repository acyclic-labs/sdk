import type { Context, Item } from "../src/index.js";
import { INFERENCE_FIXED_WIDTHS } from "../generated/widths.js";

const revisionWidth: 32 = INFERENCE_FIXED_WIDTHS.contextRevision;
const runWidth: 16 = INFERENCE_FIXED_WIDTHS.runId;
void revisionWidth;
void runWidth;
// @ts-expect-error Descriptor-derived widths preserve their exact literals.
const wrongRunWidth: 32 = INFERENCE_FIXED_WIDTHS.runId;
void wrongRunWidth;
// @ts-expect-error Generated client widths are readonly.
INFERENCE_FIXED_WIDTHS.runId = 16;

// The service returns generated Item messages. A caller's input subtype cannot
// be recovered from a revision or asserted across a server-side mutation.
declare const context: Context;
const items: Promise<readonly Item[]> = context.items();
void items;

// @ts-expect-error Context has no caller-chosen item subtype.
type ForgedContext = Context<Item & { readonly trusted: true }>;
void (undefined as unknown as ForgedContext);

// The Rust descriptor exports each terminal as a correlated readonly literal.
import { RUN_TERMINAL_METADATA, type RunTerminalMetadata } from "../generated/terminal-metadata.js";
const completedTerminal: { readonly number: 1; readonly kind: "completed"; readonly partial: false } = RUN_TERMINAL_METADATA[0];
void completedTerminal;
function terminalPolicy(value: RunTerminalMetadata): void {
  if (value.kind === "completed") {
    const number: 1 = value.number;
    const partial: false = value.partial;
    void number;
    void partial;
  }
}
void terminalPolicy;
// @ts-expect-error The Rust-derived entry cannot be modified.
RUN_TERMINAL_METADATA[0].partial = true;
// @ts-expect-error Number, kind and partial policy stay correlated.
const wrongTerminal: RunTerminalMetadata = { number: 1, kind: "cancelled", partial: false };
void wrongTerminal;
