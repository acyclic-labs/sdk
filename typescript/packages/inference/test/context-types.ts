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
