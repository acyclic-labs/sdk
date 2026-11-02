/** The original host's invocation ownership, forwarded to Rust operation::Context.
 * The host aborts `stopped` on cancellation, deadline or loss of its producer.
 * Use reason `"deadline"` for expiry; other reasons become redacted cancellation.
 * The signal must cover the supplied absolute deadline. Neither this projection
 * nor the shared client creates a timer, trust clock or allocation allowance.
 */
export interface OperationContext {
  readonly deadlineUnixMillis: bigint;
  readonly stopped: AbortSignal;
}
