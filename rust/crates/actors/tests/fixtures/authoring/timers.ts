// Generated from acyclic_actors::timers. Do not edit.
export const TIMER_STREAM_PREFIX = "harness/v2/timers";
export type TimerRecord<Task = string, Operation = string> = {
/**
 * Task identity; default authoring projection uses canonical UUID text.
 */
task_id: Task,
/**
 * Stable operation identity, fixed before scheduling.
 */
operation_id: Operation,
/**
 * Positive Unix deadline in milliseconds; readiness is now >= deadline.
 */
deadline_unix_ms: bigint, };
