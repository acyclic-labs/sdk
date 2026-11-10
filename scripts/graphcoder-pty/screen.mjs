// Inspect actual terminal cells, including cursor-addressed diff updates.
import headless from "@xterm/headless";
export const Terminal = headless.Terminal;
// Match the real visible cells, including attributes lost by text-only captures.
export function styledRun(terminal, { text, bold, fg, bg }) {
  const color = (cell, side, expected) => expected === undefined || (expected === true
    ? !cell[`is${side}Default`]() : [].concat(expected).includes(cell[`get${side}Color`]()));
  for (const line of rows(terminal)) {
    if (!line) continue;
    for (let x = 0; x <= terminal.cols - text.length; x++) {
      const cells = Array.from(text, (_, index) => line.getCell(x + index));
      if (cells.map(cell => cell?.getChars() || " ").join("") === text
          && cells.every((cell, index) => text[index] === " " || (cell
            && (bold === undefined || !!cell.isBold() === bold)
            && color(cell, "Fg", fg) && color(cell, "Bg", bg)))) return true;
    }
  }
  return false;
}
function rows(terminal, buffer = terminal.buffer.active) {
  return Array.from({ length: terminal.rows }, (_, y) => buffer.getLine(buffer.viewportY + y));
}
export function screenText(terminal) {
  return rows(terminal).map(line => line?.translateToString(true) ?? "").join("\n");
}
export function screenSnapshot(terminal, { completeLines = false } = {}) {
  const buffer = terminal.buffer.active, lines = rows(terminal, buffer);
  const visible = lines.map(line => line?.translateToString(true) ?? "").join("\n");
  if (completeLines && buffer.type === "normal") {
    // A pipe chunk is not a diagnostic boundary. Exclude the entire current
    // logical line, including any of its preceding soft-wrapped rows.
    let cursor = buffer.baseY + buffer.cursorY - buffer.viewportY;
    while (cursor > 0 && lines[cursor]?.isWrapped) cursor--;
    lines.length = Math.max(0, Math.min(lines.length, cursor));
  }
  const diagnostic = lines.map((line, index) =>
    (index === 0 || line?.isWrapped ? "" : "\n")
      + (line?.translateToString(!lines[index + 1]?.isWrapped) ?? "")).join("");
  return { visible, diagnostic };
}
