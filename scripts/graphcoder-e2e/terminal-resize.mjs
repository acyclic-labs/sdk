// One resize channel over the original terminal owner's native handle.
import assert from "node:assert/strict";

export function terminalSize(cols, rows) {
  assert.ok([cols, rows].every(value => Number.isSafeInteger(value) && value > 0 && value <= 32767),
    "terminal dimensions must fit positive native coordinates");
}

export function terminalResize(stream, closed) {
  let serial = Promise.resolve();
  let failure;
  stream.on("error", error => { failure ??= error; });
  closed.then(() => stream.destroy(), () => stream.destroy());
  function readable() {
    return new Promise((resolve, reject) => {
      const cleanup = () => { stream.off("readable", ready); stream.off("close", ended); stream.off("error", failed); };
      const ready = () => { cleanup(); resolve(); };
      const failed = error => { cleanup(); reject(error); };
      const ended = () => failed(failure ?? Error("terminal resize channel closed"));
      stream.once("readable", ready); stream.once("close", ended); stream.once("error", failed);
      if (stream.destroyed || stream.readableEnded) ended();
    });
  }
  return (cols, rows, screen, onResize) => {
    terminalSize(cols, rows);
    const request = serial.then(async () => {
      if (failure) throw failure;
      if (stream.destroyed) throw Error("terminal resize channel closed");
      const bytes = Buffer.alloc(4);
      bytes.writeUInt16LE(cols); bytes.writeUInt16LE(rows, 2);
      // Drain earlier output, then parse native redraws at the new dimensions.
      if (screen) await new Promise(resolve => screen.write("", resolve));
      screen?.resize(cols, rows);
      onResize?.(cols, rows);
      stream.write(bytes);
      let reply;
      while ((reply = stream.read(4)) === null) await readable();
      assert.deepEqual(reply, bytes, "native terminal resize acknowledgement");
    });
    serial = request;
    serial.catch(() => {});
    return request;
  };
}
