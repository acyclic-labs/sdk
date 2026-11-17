/** JSON with comments and trailing commas, without evaluating input as code. */
export function parseJsonc(text) {
  // Keep string tokens whole: comment markers and commas inside them are data.
  const uncommented = text.replace(/^\uFEFF/, "").replace(/"(?:\\[\s\S]|[^"\\])*"|\/\/[^\r\n]*|\/\*[\s\S]*?\*\//g,
    token => token.startsWith('"') ? token : " ");
  return JSON.parse(uncommented.replace(/"(?:\\[\s\S]|[^"\\])*"|,\s*(?=[}\]])/g,
    token => token.startsWith('"') ? token : ""));
}
