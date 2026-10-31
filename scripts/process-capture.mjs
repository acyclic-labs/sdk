import { spawnSync } from 'node:child_process';
import { closeSync, mkdtempSync, openSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/**
 * @overload
 * @param {string} command
 * @param {readonly string[]} args
 * @param {Omit<import('node:child_process').SpawnSyncOptionsWithStringEncoding, 'maxBuffer'>} options
 * @returns {import('node:child_process').SpawnSyncReturns<string>}
 */
/**
 * @overload
 * @param {string} command
 * @param {readonly string[]} args
 * @param {Omit<import('node:child_process').SpawnSyncOptionsWithBufferEncoding, 'maxBuffer'>} [options]
 * @returns {import('node:child_process').SpawnSyncReturns<Buffer>}
 */
/**
 * @overload
 * @param {string} command
 * @param {readonly string[]} args
 * @param {Omit<import('node:child_process').SpawnSyncOptions, 'maxBuffer'>} [options]
 * @returns {import('node:child_process').SpawnSyncReturns<string | Buffer>}
 */
/**
 * Capture a synchronous command through files rather than bounded pipe buffers.
 * The child writes without retaining its output in Node; callers receive the
 * complete selected streams after exit. Inherited/ignored/file streams stay so.
 * @param {string} command
 * @param {readonly string[]} args
 * @param {Omit<import('node:child_process').SpawnSyncOptions, 'maxBuffer'>} [options]
 * @returns {import('node:child_process').SpawnSyncReturns<string | Buffer>}
 */
export function captureCommand(command, args = [], options = {}) {
  const streams = options.stdio ?? 'pipe';
  if (!Array.isArray(streams) && streams !== 'pipe' && streams !== 'overlapped') return spawnSync(command, args, options);
  /** @type {Exclude<import('node:child_process').StdioOptions, string>} */
  const stdio = Array.isArray(streams) ? [...streams] : ['pipe', 'pipe', 'pipe'];
  const directory = mkdtempSync(join(tmpdir(), 'acyclic-capture-'));
  /** @type {{ index: number, file: string, fd: number }[]} */
  const captures = [];
  try {
    for (const index of [1, 2]) {
      const stream = stdio[index];
      if (stream !== undefined && stream !== null && stream !== 'pipe' && stream !== 'overlapped') continue;
      const file = join(directory, String(index));
      const fd = openSync(file, 'wx', 0o600);
      captures.push({ index, file, fd });
      stdio[index] = fd;
    }
    /** @type {import('node:child_process').SpawnSyncReturns<string | Buffer>} */
    const result = spawnSync(command, args, { ...options, stdio });
    if (!result.output) return result;
    for (const { index, file } of captures) {
      const bytes = readFileSync(file);
      const value = options.encoding && options.encoding !== 'buffer' ? bytes.toString(options.encoding) : bytes;
      result.output[index] = value;
      if (index === 1) result.stdout = value;
      else result.stderr = value;
    }
    return result;
  } finally {
    for (const { fd } of captures) closeSync(fd);
    rmSync(directory, { recursive: true, force: true });
  }
}

/**
 * @overload
 * @param {string} command
 * @param {readonly string[]} args
 * @param {Omit<import('node:child_process').SpawnSyncOptionsWithStringEncoding, 'maxBuffer'>} options
 * @returns {string}
 */
/**
 * @overload
 * @param {string} command
 * @param {readonly string[]} args
 * @param {Omit<import('node:child_process').SpawnSyncOptionsWithBufferEncoding, 'maxBuffer'>} [options]
 * @returns {Buffer}
 */
/**
 * @overload
 * @param {string} command
 * @param {readonly string[]} args
 * @param {Omit<import('node:child_process').SpawnSyncOptions, 'maxBuffer'>} [options]
 * @returns {string | Buffer}
 */
/**
 * Complete stdout of a command that must succeed.
 * @param {string} command
 * @param {readonly string[]} args
 * @param {Omit<import('node:child_process').SpawnSyncOptions, 'maxBuffer'>} [options]
 */
export function commandOutput(command, args = [], options = {}) {
  const result = captureCommand(command, args, options);
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw Object.assign(new Error(`${command} failed (${result.status ?? result.signal}): ${result.stderr ?? ''}`), result);
  }
  return result.stdout;
}
