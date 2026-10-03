// The first request is intentionally delayed. If a caller reuses its request
// id after cancellation, the old reply must never settle the new promise.
let buffer = "";
let seen = 0;
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => {
  buffer += chunk;
  for (;;) {
    const newline = buffer.indexOf("\n");
    if (newline < 0) return;
    const line = buffer.slice(0, newline).replace(/\r$/u, "");
    buffer = buffer.slice(newline + 1);
    if (line.trim() === "") continue;
    const request = JSON.parse(line);
    seen += 1;
    const tag = seen === 1 ? "old" : "new";
    const delay = seen === 1 ? 80 : 0;
    setTimeout(() => {
      process.stdout.write(`${JSON.stringify({ request_id: request.request_id, ok: true, result: { tag } })}\n`);
    }, delay);
  }
});
