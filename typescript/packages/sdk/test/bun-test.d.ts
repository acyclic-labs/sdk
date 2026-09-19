declare module "bun:test" {
  export function test(name: string, body: () => unknown | Promise<unknown>): void;
  export function expect<Value>(value: Value): {
    toBe(expected: unknown): void;
    toEqual(expected: unknown): void;
  };
}

declare const Bun: {
  file(path: string | URL): { arrayBuffer(): Promise<ArrayBuffer> };
};
