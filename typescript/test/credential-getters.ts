/** The three JavaScript property shapes accepted by declared credential options. */
export const credentialGetterKinds = ["enumerable", "non-enumerable", "prototype"] as const;
export type CredentialGetterKind = typeof credentialGetterKinds[number];

/** Read once; any later read changes provider selection and must fail assertions. */
export function credentialGetter<T extends object, C>(options: T, callback: C, kind: CredentialGetterKind): T & { readonly credential: C } & { credentialReads(): number } {
  let reads = 0;
  const target = kind === "prototype" ? Object.create(Object.getPrototypeOf(options)) : options;
  Object.defineProperty(target, "credential", { enumerable: kind === "enumerable", get: () => ++reads === 1 ? callback : undefined });
  if (kind === "prototype") Object.setPrototypeOf(options, target);
  Object.defineProperty(options, "credentialReads", { value: () => reads });
  return options as T & { readonly credential: C } & { credentialReads(): number };
}
