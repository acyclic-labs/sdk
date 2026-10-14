/** Consumed API of the pinned yaml 2.9.1 runtime bundle. */
declare const YAML: {
  parse(source: string): any;
  parseDocument(source: string, options?: { merge?: boolean }): {
    readonly errors: readonly Error[];
    toJS(options?: { maxAliasCount?: number }): unknown;
  };
};
export = YAML;
