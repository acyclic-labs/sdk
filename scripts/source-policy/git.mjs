// Decode the original Git batch response once, retaining only requested sources.
export class GitBatchSyntaxError extends Error {
 constructor(message) { super(message); this.name = 'GitBatchSyntaxError'; }
}

export function gitBatchSources(files, original) {
 let at = 0;
 const sources = new Map();
 for (const file of files) {
  const headerEnd = original.indexOf(10, at);
  if (headerEnd < 0) throw new GitBatchSyntaxError('truncated Git object header for ' + file);
  const header = original.subarray(at, headerEnd).toString('utf8');
  at = headerEnd + 1;
  if (!header.endsWith(' missing')) {
   if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64}) blob (?:0|[1-9][0-9]*)$/.test(header)) throw new GitBatchSyntaxError('invalid Git object header for ' + file);
   const fields = header.split(' ');
   const size = Number(fields[2]);
   if (!Number.isSafeInteger(size) || at + size >= original.length || original[at + size] !== 10) throw new GitBatchSyntaxError('invalid Git object for ' + file);
   const before = original.subarray(at, at + size);
   at += size + 1;
   sources.set(file, before.toString('utf8'));
  }
 }
 if (at !== original.length) throw new GitBatchSyntaxError('unconsumed Git object response');
 return sources;
}
