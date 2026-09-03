/** Ordered stream record. */
export interface StreamRecord { readonly offset: number; readonly payload: Uint8Array }
/** Public ordered stream provider contract. */
export interface StreamProvider { append(stream: string, expectedOffset: number, payload: Uint8Array): Promise<StreamRecord>; read(stream: string, from: number): Promise<readonly StreamRecord[]> }
/** Deterministic process-local stream provider. */
export class MemoryStream implements StreamProvider {
  readonly #streams = new Map<string, StreamRecord[]>();
  async append(stream: string, expectedOffset: number, payload: Uint8Array): Promise<StreamRecord> {
    const values = this.#streams.get(stream) ?? [];
    if (values.length !== expectedOffset) throw new Error(`conflict: ${stream}`);
    const value = { offset: expectedOffset, payload: payload.slice() };
    values.push(cloneRecord(value)); this.#streams.set(stream, values); return cloneRecord(value);
  }
  async read(stream: string, from: number): Promise<readonly StreamRecord[]> { return (this.#streams.get(stream) ?? []).filter(value => value.offset >= from).map(cloneRecord); }
}
const cloneRecord = (record: StreamRecord): StreamRecord => ({ offset: record.offset, payload: record.payload.slice() });
