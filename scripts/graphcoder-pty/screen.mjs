// Inspect actual terminal cells, including cursor-addressed diff updates.
import headless from "@xterm/headless";
export const Terminal = headless.Terminal;
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
export function hasBoldAnswer(terminal) {
  const answer = "test answer";
  return rows(terminal).some(line => {
    if (!line) return false;
    for (let x = 0; x <= terminal.cols - answer.length; x++) {
      const cells = Array.from({ length: answer.length }, (_, offset) => line.getCell(x + offset));
      if (cells.every(Boolean) && cells.map(cell => cell.getChars() || " ").join("") === answer
          && cells.every((cell, index) => answer[index] === " " || cell.isBold())) return true;
    }
    return false;
  });
}

export function thinkingPhase(terminal) {
  for (const line of rows(terminal)) {
    if (!line) continue;
    const x = line.translateToString(true).indexOf("Thinking…");
    if (x < 0) continue;
    return Array.from({ length: 9 }, (_, offset) => line.getCell(x + offset)?.isBold())
      .findIndex(Boolean);
  }
  return -1;
}
