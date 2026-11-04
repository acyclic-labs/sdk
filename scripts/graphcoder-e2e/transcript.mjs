import { close, createWriteStream, write, writev } from "node:fs";

// Pipe the original bytes with stream backpressure. Completion owns the
// actual file close, including failure; finish alone still owns a descriptor.
// A failed file drains the source so its producer cannot block on a full pipe.
export function transcript(stream, fd, observe, channel) {
  let first = true, failure;
  const completed = observe ? callback => (error, count, buffers) => {
    if (!error && count > 0 && first) { first = false; observe(`${channel}-first-write`); }
    callback(error, count, buffers);
  } : undefined;
  const file = createWriteStream(null, { fd, ...(observe ? { fs: {
    close,
    write(fd, buffer, offset, length, position, callback) {
      return write(fd, buffer, offset, length, position, first ? completed(callback) : callback);
    },
    writev(fd, buffers, position, callback) {
      return writev(fd, buffers, position, first ? completed(callback) : callback);
    },
  } } : {}) });
  const closed = new Promise((resolve, reject) => {
    file.once("error", error => { failure ??= error; stream.unpipe(file); stream.resume(); });
    file.once("finish", () => observe?.(`${channel}-finish`));
    file.once("close", () => {
      if (!file.writableFinished) {
        failure ??= Object.assign(new Error(
          "STEP_FAILED transcript-close: file withdrew before finish; recovery: node --test scripts/graphcoder-e2e/transcript.test.mjs",
        ), { code: "ERR_STREAM_PREMATURE_CLOSE" });
        stream.unpipe(file); stream.resume();
      }
      if (failure) reject(failure); else resolve();
    });
    stream.once("error", error => { failure ??= error; file.destroy(error); });
    stream.once("end", () => observe?.(`${channel}-eof`));
    stream.once("close", () => {
      observe?.(`${channel}-close`);
      if (!stream.readableEnded) file.end();
    });
  });
  stream.pipe(file);
  return closed;
}
