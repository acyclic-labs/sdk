import type {
  WasmRawChangeSet,
  WasmRawCheckout,
  WasmRawFs,
  WasmRawGeneration,
  WasmRawJoinPlan,
  WasmRawResolvedFile,
  WasmRawResolvedFiles,
  WasmRawSpeculation,
  WasmRawTransaction,
  WasmRawVolume,
  WasmRawWorkspace,
  CheckoutOptions,
  JoinOptions,
  WorkspaceCommit,
} from "../src/contracts.js";

type IsAny<Value> = 0 extends (1 & Value) ? true : false;
type AnyMethodNames<Value> = {
  [Key in keyof Value]: Value[Key] extends (...args: infer _Args) => infer Result
    ? IsAny<Result> extends true ? Key : IsAny<Awaited<Result>> extends true ? Key : never
    : never;
}[keyof Value];
type AssertNoAny<Value extends never> = Value;
type IsUnknown<Value> = IsAny<Value> extends true
  ? false
  : unknown extends Value
    ? [keyof Value] extends [never] ? true : false
    : false;
type IsAnyOrUnknown<Value> = IsAny<Value> extends true ? true : IsUnknown<Value>;
type AnyScanDepth = [unknown, unknown, unknown, unknown, unknown, unknown];
type ContainsAnyOrUnknown<Value, Depth extends readonly unknown[] = AnyScanDepth> =
  IsAnyOrUnknown<Value> extends true
    ? true
    : [Value] extends [never]
      ? false
      : Depth extends readonly [unknown, ...infer Rest]
      ? Value extends PromiseLike<infer Inner>
        ? ContainsAnyOrUnknown<Inner, Rest>
        : Value extends readonly (infer Element)[]
          ? ContainsAnyOrUnknown<Element, Rest>
          : Value extends ArrayBufferView
            ? false
            : Value extends (...args: infer _Args) => infer _Result
              ? false
              : Value extends object
                ? true extends {
                    [Key in keyof Value]-?: ContainsAnyOrUnknown<Value[Key], Rest>
                  }[keyof Value]
                  ? true
                  : false
                : false
      : false;
type AnyOrUnknownMethodResults<Value> = {
  [Key in keyof Value]: Value[Key] extends (...args: infer _Args) => infer Result
    ? ContainsAnyOrUnknown<Result> extends true ? Key
      : ContainsAnyOrUnknown<Awaited<Result>> extends true ? Key : never
    : never;
}[keyof Value];
type AnyOrUnknownMethodParameters<Value> = {
  [Key in keyof Value]: Value[Key] extends (...args: infer Args) => unknown
    ? ContainsAnyOrUnknown<Args[number]> extends true ? Key : never
    : never;
}[keyof Value];
type AnyOrUnknownPropertyNames<Value> = {
  [Key in keyof Value]: Value[Key] extends (...args: infer _Args) => unknown
    ? never
    : ContainsAnyOrUnknown<Value[Key]> extends true ? Key : never;
}[keyof Value];
type AssertExtends<Actual, Expected> = [Actual] extends [Expected] ? true : never;

export type _FsResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawFs>>;
export type _WorkspaceResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawWorkspace>>;
export type _GenerationResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawGeneration>>;
export type _ChangeSetResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawChangeSet>>;
export type _JoinResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawJoinPlan>>;
export type _TransactionResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawTransaction>>;
export type _SpeculationResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawSpeculation>>;
export type _VolumeResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawVolume>>;
export type _CheckoutResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawCheckout>>;
export type _ResolvedFileResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawResolvedFile>>;
export type _ResolvedFilesResultsAreTyped = AssertNoAny<AnyMethodNames<WasmRawResolvedFiles>>;
export type _FsMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawFs>>;
export type _WorkspaceMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawWorkspace>>;
export type _GenerationMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawGeneration>>;
export type _ChangeSetMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawChangeSet>>;
export type _JoinMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawJoinPlan>>;
export type _TransactionMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawTransaction>>;
export type _SpeculationMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawSpeculation>>;
export type _VolumeMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawVolume>>;
export type _CheckoutMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawCheckout>>;
export type _ResolvedFileMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawResolvedFile>>;
export type _ResolvedFilesMethodsHaveKnownResults = AssertNoAny<AnyOrUnknownMethodResults<WasmRawResolvedFiles>>;

export type _FsMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawFs>>;
export type _WorkspaceMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawWorkspace>>;
export type _GenerationMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawGeneration>>;
export type _ChangeSetMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawChangeSet>>;
export type _JoinMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawJoinPlan>>;
export type _TransactionMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawTransaction>>;
export type _SpeculationMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawSpeculation>>;
export type _VolumeMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawVolume>>;
export type _CheckoutMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawCheckout>>;
export type _ResolvedFileMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawResolvedFile>>;
export type _ResolvedFilesMethodsHaveKnownParameters = AssertNoAny<AnyOrUnknownMethodParameters<WasmRawResolvedFiles>>;

export type _FsPropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawFs>>;
export type _WorkspacePropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawWorkspace>>;
export type _GenerationPropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawGeneration>>;
export type _ChangeSetPropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawChangeSet>>;
export type _JoinPropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawJoinPlan>>;
export type _TransactionPropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawTransaction>>;
export type _SpeculationPropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawSpeculation>>;
export type _VolumePropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawVolume>>;
export type _CheckoutPropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawCheckout>>;
export type _ResolvedFilePropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawResolvedFile>>;
export type _ResolvedFilesPropertiesAreKnown = AssertNoAny<AnyOrUnknownPropertyNames<WasmRawResolvedFiles>>;

export type _WorkspaceJoinParameters = AssertExtends<
  Parameters<WasmRawWorkspace["joinInto"]>,
  [target: WasmRawWorkspace, options: JoinOptions]
>;
export type _VolumeCheckoutParameters = AssertExtends<
  Parameters<WasmRawVolume["checkout"]>,
  [options: CheckoutOptions]
>;
export type _WorkspaceWriteResult = AssertExtends<
  Awaited<ReturnType<WasmRawWorkspace["write"]>>,
  WorkspaceCommit
>;
export type _WorkspaceRemoveResult = AssertExtends<
  Awaited<ReturnType<WasmRawWorkspace["remove"]>>,
  WorkspaceCommit
>;

export {};
