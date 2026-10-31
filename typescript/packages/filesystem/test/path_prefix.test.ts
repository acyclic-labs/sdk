import { expect, test } from "bun:test";
import { PathPrefixIndex } from "../src/path_prefix.js";

test("component prefix traversal stays within query work after unrelated bindings", () => {
  const deep = `/work/${"segment/".repeat(64)}leaf`;
  const bindings = ["/", "/work", "/work/深い", "/work/🦀", deep];
  const queries = ["/", "/file", "/work", "/worker/file", "/work/深い/file", "/work/深い別/file",
    "/work/🦀/file", "/work/🦁/file", deep, `${deep}/file`, `${deep}other/file`];
  const charCodeAt = String.prototype.charCodeAt;
  const measured = (index: PathPrefixIndex<string>, query: string) => {
    let visits = 0;
    String.prototype.charCodeAt = function (offset: number) {
      visits += 1;
      return charCodeAt.call(this, offset);
    };
    try { return { value: index.longest(query), visits }; }
    finally { String.prototype.charCodeAt = charCodeAt; }
  };
  const scanned = (paths: readonly string[], query: string) => {
    let value: string | undefined;
    let visits = 0;
    for (const path of paths) {
      let part = 0;
      while (part < path.length && part < query.length) {
        visits += 1;
        if (path.charCodeAt(part) !== query.charCodeAt(part)) break;
        part += 1;
      }
      if (part === path.length && (part === 1 || part === query.length || query[part] === "/")
        && (value === undefined || path.length > value.length)) value = path;
    }
    return { value, visits };
  };
  for (const reverse of [false, true]) {
    const paths = reverse ? [...bindings].reverse() : [...bindings];
    const index = new PathPrefixIndex<string>();
    for (const path of paths) index.insert(path, path);
    for (const query of queries) {
      const result = measured(index, query);
      expect(result.value).toBe(scanned(paths, query).value);
      expect(result.visits).toBeLessThanOrEqual(4 * (query.length + 1));
    }
    for (let binding = 0; binding < 4096; binding += 1) {
      const path = `/foreign/${String.fromCharCode(0x1000 + binding)}/root`;
      paths.push(path);
      index.insert(path, path);
    }
    for (const query of queries) {
      const result = measured(index, query);
      expect(result.value).toBe(scanned(paths, query).value);
      expect(result.visits).toBeLessThanOrEqual(4 * (query.length + 1));
    }
    expect(scanned(paths, "/work/file").visits).toBeGreaterThan(4 * ("/work/file".length + 1));
    expect(index.insert("/work/深い", "replacement")).toBe("/work/深い");
    expect(index.longest("/work/深い/file")).toBe("replacement");
    expect(index.longest("/work/深い別/file")).toBe("/work");
  }
});
