import { screenSnapshot as terminalSnapshot } from "../graphcoder-pty/screen.mjs";

// Cursor-positioned redraws do not set xterm's automatic-wrap flag.
// GraphCoder's transcript reserves the last column for its scrollbar and
// repeats its indentation on continuation rows.
function logicalText(lines, columns) {
  let text = "", previous, previousBox = false;
  for (let y = 0; y < lines.length; y++) {
    const line = lines[y];
    const box = line?.getCell(0)?.getChars() === "│"
      && line?.getCell(columns - 1)?.getChars() === "│";
    // Dialog contents wrap inside their border, not at the terminal edge.
    // Keep the visible/diagnostic snapshots exact; only this logical view
    // removes the frame before joining a full interior continuation row.
    const span = line?.translateToString(true, box ? 1 : 0, box ? columns - 1 : columns) ?? "";
    // xterm trims against the whole line, where the right border is still
    // nonblank. Trim padding within the selected interior separately.
    const row = box ? span.trimEnd() : span;
    const filled = column => previous?.getCell(column)?.getChars().trim();
    const continued = box || previousBox
      ? box && previousBox && filled(columns - 2)
      : line?.isWrapped || filled(columns - 1) || filled(columns - 2);
    text += (y && !continued ? "\n" : "") + (continued ? row.trimStart() : row);
    previous = line;
    previousBox = box;
  }
  return text;
}


export function screenSnapshot(screen, options) {
  const snapshot = terminalSnapshot(screen, options);
  return { ...snapshot, logical: logicalText(snapshot.lines, screen.cols) };
}
