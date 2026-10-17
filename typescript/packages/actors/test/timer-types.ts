import { schedule, setAlarm, type TimerRecord } from "../src/index.js";

const timer: TimerRecord<string, string> = { task_id: "task", operation_id: "operation", deadline_unix_ms: 1n };
const retain = async (_timer: Readonly<TimerRecord>, _encoded: Uint8Array) => 1;
const result: Promise<number> = schedule(retain, timer);
const alarm: typeof schedule = setAlarm;
void result;
void alarm;
// @ts-expect-error Milliseconds are lossless bigint, not a rounded JS number.
schedule(retain, { ...timer, deadline_unix_ms: 1 });
