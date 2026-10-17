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
