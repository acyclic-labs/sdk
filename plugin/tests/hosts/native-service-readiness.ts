import { existsSync, readFileSync } from "node:fs";
import type { WatchEventType } from "node:fs";

/** Directory notifications are hints: an atomic rename may report only its staged name. */
export function nativeServiceReadinessProbe(marker: string, previous: string, ready: () => void) {
  return (_event: WatchEventType, _filename: string | Buffer | null) => {
    if (existsSync(marker) && readFileSync(marker, "utf8") !== previous) ready();
  };
}
