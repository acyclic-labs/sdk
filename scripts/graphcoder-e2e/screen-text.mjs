import { screenSnapshot as terminalSnapshot } from "../graphcoder-pty/screen.mjs";

// Cursor-positioned redraws do not set xterm's automatic-wrap flag.
// GraphCoder's transcript reserves the last column for its scrollbar and
// repeats its indentation on continuation rows.
function logicalText(lines, columns) {
  let text = "", previous;
  for (let y = 0; y < lines.length; y++) {
    const line = lines[y];
    const row = line?.translateToString(true) ?? "";
    const filled = column => previous?.getCell(column)?.getChars().trim();
    const continued = line?.isWrapped || filled(columns - 1)
      || filled(columns - 2);
    text += (y && !continued ? "\n" : "") + (continued ? row.trimStart() : row);
    previous = line;
  }
  return text;
}


export function screenSnapshot(screen, options) {
  const snapshot = terminalSnapshot(screen, options);
  return { ...snapshot, logical: logicalText(snapshot.lines, screen.cols) };
}
