import { DemandLoader, PageWindow, RequestScheduler, type FileRef, type PageLoader,
  type ConversationMessage } from "../src/index.js";

/** Reference-first reader composition. Page and body providers authenticate
 * pins/grants before IO using their existing contracts. Neither construction
 * nor paging fetches any message body. The owner supplies all resource bounds.
 */
export function conversationDemand(options: {
  scheduler: RequestScheduler;
  loadPage: PageLoader<ConversationMessage, bigint>;
  /** Provider-enforced maximum deep resident metadata for one page request. */
  pageBytes: number;
  windowMessages: number;
  readBody: (file: FileRef) => Promise<Uint8Array>;
  demand: number;
  bodyEntries: number;
  bodyBytes: number;
}) {
  const window = new PageWindow<ConversationMessage, bigint>(message => message.id, (direction, cursor) =>
    options.scheduler.schedule(options.pageBytes, () => options.loadPage(direction, cursor)), options.windowMessages);
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const bodies = new DemandLoader<FileRef, string>({
    // UTF-16 text costs at most twice its UTF-8 input. Reserve the input and
    // output simultaneously plus fixed value bookkeeping for this consumer.
    // Pinned normalized metadata is owned/accounted by the reference window.
    reserve: file => file.descriptor.byte_length * 3 + 96,
    load: async file => {
      // Existing verified FileRef reader enforces byte_length before buffering.
      // This adapter may ignore abort; unsettled IO still owns scheduler capacity.
      const bytes = await options.readBody(file);
      if (bytes.buffer.byteLength > file.descriptor.byte_length) throw new RangeError("body backing buffer exceeds descriptor");
      const value = decoder.decode(bytes);
      return { value, bytes: value.length * 2 + 96 };
    },
  }, options.scheduler, options.demand, options.bodyEntries, options.bodyBytes);
  return { window, bodies };
}
