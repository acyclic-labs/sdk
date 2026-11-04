// Inspect actual terminal cells, including cursor-addressed diff updates.
import headless from "@xterm/headless";
export const Terminal = headless.Terminal;
function rows(terminal) {
  const buffer = terminal.buffer.active;
  return Array.from({ length: terminal.rows }, (_, y) => buffer.getLine(buffer.viewportY + y));
}
export function screenText(terminal) {
  return rows(terminal).map(line => line?.translateToString(true) ?? "").join("\n");
}
