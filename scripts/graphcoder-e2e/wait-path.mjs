import { statSync, watch } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

function present(path) {
  try {
    return statSync(path).isFile();
  } catch (error) {
    if (error.code === 'ENOENT' || error.code === 'ENOTDIR') return false;
    throw error;
  }
}

// One native filesystem subscription, armed before the first observation.
// Missing intermediate directories are covered by the recursive ancestor watch.
// Rechecks happen only after actual filesystem events; the timer is a caller's
// failure deadline, never an interval or an application readiness source.
export function waitForFile(path, options) {
  return waitForFileValue(path, () => true, options).then(() => {});
}

// The predicate may return undefined while a producer is still writing. Its
// next invocation follows a native filesystem event, with no periodic retry.
export function waitForFileValue(path, read, { timeoutMs, signal } = {}) {
  path = resolve(path);
  return new Promise((accept, reject) => {
    let subscription;
    let deadline;
    const finish = (error, value) => {
      subscription?.close();
      clearTimeout(deadline);
      signal?.removeEventListener('abort', aborted);
      if (error) reject(error);
      else accept(value);
    };
    const aborted = () => finish(signal.reason ?? new Error('file wait cancelled'));
    const observe = () => {
      try {
        if (present(path)) {
          const value = read(path);
          if (value !== undefined) finish(undefined, value);
        }
      } catch (error) { finish(error); }
    };
    if (signal?.aborted) return aborted();
    try {
      let ancestor = dirname(path);
      while (true) {
        try {
          if (!statSync(ancestor).isDirectory()) throw new Error(`not a directory: ${ancestor}`);
          break;
        } catch (error) {
          if (error.code !== 'ENOENT') throw error;
          const parent = dirname(ancestor);
          if (parent === ancestor) throw error;
          ancestor = parent;
        }
      }
      subscription = watch(ancestor, { recursive: true }, observe);
      subscription.on('error', finish);
      signal?.addEventListener('abort', aborted, { once: true });
      if (timeoutMs !== undefined) {
        deadline = setTimeout(() => finish(new Error(`file readiness deadline: ${path}`)), timeoutMs);
      }
      observe();
    } catch (error) { finish(error); }
  });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [path, seconds] = process.argv.slice(2);
  if (!path || (seconds !== undefined && (!Number.isFinite(Number(seconds)) || !(Number(seconds) > 0)))) {
    throw new Error('wait-path PATH [DEADLINE_SECONDS]');
  }
  await waitForFile(path, { timeoutMs: seconds === undefined ? undefined : Number(seconds) * 1000 });
}
