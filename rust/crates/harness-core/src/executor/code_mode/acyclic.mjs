// The module is instantiated with the same admitted RPC bridge as native tools.
export class ConflictError extends Error {
    constructor(path, current) {
        super(`File changed: ${path}`);
        this.name = 'ConflictError';
        this.path = path;
        this.current = current;
    }
}
export class TimeoutError extends Error {
    constructor() { super('Timeout passed'); this.name = 'TimeoutError'; }
}
export class DeniedError extends Error {
    constructor(reason) { super(reason); this.name = 'DeniedError'; this.reason = reason; }
}

const rpc = globalThis.__acyclic;
delete globalThis.__acyclic;
const bind = async (name, input) => {
    try { return await rpc(name, input); }
    catch (error) {
        if (error.class === 'not_found' && name === 'read') return null;
        if (error.class === 'conflict') throw new ConflictError(input.path, await read(input.path));
        if (error.class === 'denied' || error.class === 'unauthorized') throw new DeniedError(error.message);
        throw error;
    }
};
const parse = JSON.parse, stringify = JSON.stringify;
const encode = (path, data) => typeof data === 'string' ? data
    : `${path.endsWith('.ts') ? 'export default ' : ''}${stringify(data)}`;

// Each value holds one immutable version. Byte pages stay pinned to that
// version; text()/json()/lines() never issue hidden host calls.
class File {
    #text;
    #starts;
    constructor(path, version, size, text) {
        this.path = path;
        this.version = version;
        this.size = size;
        this.#text = text;
        this.#starts = text.length ? [0] : [];
        for (let i = 0; i < text.length; i++) {
            if (text.charCodeAt(i) === 10 && i + 1 < text.length) this.#starts.push(i + 1);
        }
        Object.freeze(this);
    }
    text() { return this.#text; }
    json() { return parse(this.#text); }
    lines(from, count) {
        if (!Number.isSafeInteger(from) || from < 0 || !Number.isSafeInteger(count) || count < 0)
            throw new RangeError('lines requires nonnegative safe integers');
        const out = [];
        for (let i = from, end = Math.min(this.#starts.length, from + count); i < end; i++) {
            let end = i + 1 < this.#starts.length ? this.#starts[i + 1] - 1 : this.#text.length;
            if (end > this.#starts[i] && this.#text.charCodeAt(end - 1) === 10) end--;
            if (end > this.#starts[i] && this.#text.charCodeAt(end - 1) === 13) end--;
            out.push(this.#text.slice(this.#starts[i], end));
        }
        return out;
    }
}

export async function read(path) {
    let result = await bind('read', {path, selection: {kind: 'text'}});
    if (result === null) return null;
    const version = result.version;
    const size = result.file.authored ? result.file.authored.byte_length
        : result.file.content.descriptor.byte_length;
    if (result.media) throw new DeniedError('File requires a text read');
    const parts = [result.text];
    while (result.page?.next != null) {
        const start = result.page.next;
        result = await bind('read', {path, version, range: {start, end: size}});
        if (result.page?.next != null && result.page.next <= start)
            throw new Error('File read did not advance');
        parts.push(result.text);
    }
    return new File(path, version, size, parts.join(''));
}

export async function write(path, data, ifVersion) {
    if (typeof ifVersion !== 'string' || ifVersion.length === 0)
        throw new TypeError('write requires any, absent or a version');
    const result = await bind('write', {path, content: encode(path, data), ifVersion});
    return result.version;
}

export async function spawn() { throw new DeniedError('needs P2'); }
export async function fork() { throw new DeniedError('needs P2'); }
