import type { Context, Item } from "../src/index.js";

// The service returns generated Item messages. A caller's input subtype cannot
// be recovered from a revision or asserted across a server-side mutation.
declare const context: Context;
const items: Promise<readonly Item[]> = context.items();
void items;

// @ts-expect-error Context has no caller-chosen item subtype.
type ForgedContext = Context<Item & { readonly trusted: true }>;
void (undefined as unknown as ForgedContext);
