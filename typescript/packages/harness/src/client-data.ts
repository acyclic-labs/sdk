import { type ClientCommand } from "./client.js";
import { isSafeAuthorityId } from "./rust-policy.js";
import { NativeContracts } from "./native-contracts.js";

export function retainedDataBytes(value: unknown, maximum: number, allowUndefined = false): number {
  let bytes = 0;
  const ancestors = new Set<object>();
  const charge = (amount: number): void => {
    if (amount > maximum - bytes) throw new RangeError("client command byte capacity exceeded");
    bytes += amount;
  };
  const visit = (data: unknown, depth: number): void => {
    if (depth > 64) throw new RangeError("client command nesting exceeded");
    charge(32);
    if (data === undefined && allowUndefined) return;
    if (typeof data === "string") { charge(data.length * 2); return; }
    if (data === null || typeof data === "boolean" || typeof data === "number") return;
    if (typeof data === "bigint") { charge(data.toString().length * 2); return; }
    if (typeof data !== "object") throw new TypeError("command contains a non-data value");
    if (ancestors.has(data)) throw new TypeError("command contains a cycle");
    if (data instanceof ArrayBuffer) { charge(data.byteLength); return; }
    if (ArrayBuffer.isView(data)) { charge(data.buffer.byteLength); return; }
    if (Array.isArray(data)) charge(data.length * 8);
    if (!Array.isArray(data) && Object.getPrototypeOf(data) !== Object.prototype && Object.getPrototypeOf(data) !== null) {
      throw new TypeError("command contains a non-canonical structured value");
    }
    ancestors.add(data);
    try {
      for (const key in data) {
        if (!Object.hasOwn(data, key)) continue;
        charge(32 + key.length * 2);
        const descriptor = Object.getOwnPropertyDescriptor(data, key)!;
        if (!("value" in descriptor)) throw new TypeError("command contains an accessor");
        visit(descriptor.value, depth + 1);
      }
    } finally { ancestors.delete(data); }
  };
  visit(value, 0);
  return bytes;
}

export async function assertOutboxSafe(command: ClientCommand): Promise<ClientCommand> {
  const admitted = cloneStructuredValue(command, new Set()) as ClientCommand;
  if (admitted.offlineSafe !== true || admitted.kind === "interaction.resolve.approval") {
    throw new TypeError("command is not safe for the offline outbox");
  }
  const fields = Object.keys(admitted);
  if (fields.length !== 5 || fields.some(field => !["operationId", "authority", "kind", "payload", "offlineSafe"].includes(field))) {
    throw new TypeError("offline outbox command contains an unsupported field");
  }
  if (admitted.authority === null || typeof admitted.authority !== "object"
    || Object.getPrototypeOf(admitted.authority) !== Object.prototype
    || Object.keys(admitted.authority).length !== 2
    || !Object.keys(admitted.authority).every(field => field === "kind" || field === "id")) {
    throw new TypeError("offline outbox authority contains an unsupported field");
  }
  if (!(await isSafeAuthorityId(admitted.authority.id))) {
    throw new TypeError("offline outbox authority contains an unsafe identity");
  }
  const forbidden = /(?:token|authorization|credential|secret|password|api[_-]?key|(?:^|[_-])(?:scope|proof|body|text|bytes|base64|data)(?:$|[_-]))/i;
  const visit = (value: unknown): void => {
    if (value === null || typeof value !== "object") return;
    if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
      throw new TypeError("offline outbox cannot persist inline bytes or credentials");
    }
    for (const [key, item] of Object.entries(value)) {
      if (forbidden.test(key)) throw new TypeError("offline outbox cannot persist inline bytes or credentials");
      visit(item);
    }
  };
  visit(admitted);
  const payload = admitted.payload;
  if (payload === null || typeof payload !== "object" || Array.isArray(payload) ||
      Object.getPrototypeOf(payload) !== Object.prototype) {
    throw new TypeError("offline outbox payload must be a ref-only record");
  }
  for (const key of Object.keys(payload)) {
    if (!["content", "attachments", "artifacts", "references", "metadata"].includes(key)) {
      throw new TypeError("offline outbox payload contains an unsupported field");
    }
  }
  const contracts = await NativeContracts.create();
  if (payload.content !== undefined) contracts.validate("file_ref", payload.content);
  if (payload.attachments !== undefined) contracts.validate("attachments", payload.attachments);
  for (const refs of [payload.artifacts, payload.references]) {
    if (refs === undefined) continue;
    if (!Array.isArray(refs)) throw new TypeError("offline outbox references must be a list");
    for (const reference of refs) contracts.validate("file_ref", reference);
  }
  if (payload.metadata !== undefined) {
    if (payload.metadata === null || typeof payload.metadata !== "object" || Array.isArray(payload.metadata) ||
        Object.getPrototypeOf(payload.metadata) !== Object.prototype) {
      throw new TypeError("offline outbox metadata must be a record");
    }
    for (const [key, value] of Object.entries(payload.metadata)) {
      if (forbidden.test(key) || !(value === null || ["number", "boolean", "bigint"].includes(typeof value))) {
        throw new TypeError("offline outbox metadata contains an unsafe value");
      }
    }
  }
  return admitted;
}

export function cloneStructuredValue(value: unknown, ancestors: Set<object>, allowUndefined = false, canonical = false): unknown {
  if (value === undefined && allowUndefined) return value;
  if (
    value === null || typeof value === "string" || typeof value === "boolean"
  ) return value;
  if (typeof value === "bigint") return canonical ? { $bigint: value.toString() } : value;
  if (typeof value === "number") {
    if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) {
      throw new TypeError("command contains a non-finite number or inexact integer");
    }
    return value;
  }
  if (typeof value !== "object") throw new TypeError("command contains a non-data value");
  if (ancestors.has(value)) throw new TypeError("command contains a cycle");
  if (value instanceof ArrayBuffer) return canonical ? { $bytes: [...new Uint8Array(value)] } : value.slice(0);
  if (value instanceof Uint8Array) return canonical ? { $bytes: [...value] } : value.slice();
  if (ArrayBuffer.isView(value)) {
    throw new TypeError("command contains an unsupported binary view");
  }
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw new TypeError("command contains a non-canonical structured value");
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const keys = Reflect.ownKeys(descriptors);
  if (keys.some(key => typeof key !== "string")) {
    throw new TypeError("command contains symbol properties");
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const cloned = new Array<unknown>(value.length);
      for (const key of keys as string[]) {
        if (key === "length" || descriptors[key]?.enumerable !== true) continue;
        const index = Number(key);
        if (!Number.isSafeInteger(index) || index < 0 || index >= value.length || String(index) !== key) {
          throw new TypeError("command array contains custom properties");
        }
        const descriptor = descriptors[key];
        if (descriptor === undefined || !("value" in descriptor)) {
          throw new TypeError("command contains an accessor");
        }
        cloned[index] = cloneStructuredValue(descriptor.value, ancestors, allowUndefined, canonical);
      }
      return cloned;
    }
    const cloned: Record<string, unknown> = {};
    for (const key of keys as string[]) {
      const descriptor = descriptors[key];
      if (descriptor?.enumerable !== true) continue;
      if (!("value" in descriptor)) throw new TypeError("command contains an accessor");
      Object.defineProperty(cloned, key, { value: cloneStructuredValue(descriptor.value, ancestors, allowUndefined, canonical),
        enumerable: true, configurable: true, writable: true });
    }
    return cloned;
  } finally {
    ancestors.delete(value);
  }
}
