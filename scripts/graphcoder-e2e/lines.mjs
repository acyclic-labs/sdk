import { StringDecoder } from "node:string_decoder";

// Scan each decoded chunk once. A line spanning chunks joins its fragments
// only when complete, instead of copying and rescanning every earlier prefix.
export class Lines {
  #decoder = new StringDecoder("utf8");
  #parts = [];
  #accept;
  constructor(accept) { this.#accept = accept; }
  write(chunk) { return this.#text(this.#decoder.write(chunk)); }
  #text(text) {
    let start = 0, room = true;
    for (;;) {
      const end = text.indexOf("\n", start);
      if (end === -1) {
        if (start < text.length) this.#parts.push(text.slice(start));
        return room;
      }
      let line = text.slice(start, end);
      if (this.#parts.length) {
        this.#parts.push(line);
        line = this.#parts.join("");
        this.#parts = [];
      }
      if (this.#accept(line) === false) room = false;
      start = end + 1;
    }
  }
  end({ requireNewline = false } = {}) {
    const room = this.#text(this.#decoder.end());
    if (!this.#parts.length) return room;
    if (requireNewline) {
      this.#parts = [];
      throw Object.assign(new Error("step=decode complete line; recovery=node --test scripts/graphcoder-e2e/decoder-fuzz.test.mjs"),
        { code: "UNTERMINATED_LINE" });
    }
    const tail = this.#parts.join("");
    this.#parts = [];
    return this.#accept(tail) !== false && room;
  }
}
