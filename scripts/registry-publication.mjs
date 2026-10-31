// Registry indexing has no push subscription in the publication commands.
// Keep the caller's exact resume identity instead of waiting for propagation.
export class RegistryPublicationUnobservable extends Error {
  constructor(registry, name, version, observed) {
    const recovery = publicationCommand();
    super(`step=${registry}-publication-visibility ${name}@${version} is unobservable: ${JSON.stringify(observed)}; recovery command: ${recovery}`);
    this.name = "RegistryPublicationUnobservable";
    this.code = "REGISTRY_PUBLICATION_UNOBSERVABLE";
    this.registry = registry;
    this.package = name;
    this.version = version;
    this.observed = observed;
    this.recovery = recovery;
  }
}

// These commands are printed for the caller, never executed by a shell here.
export function publicationCommand(command = process.execPath, args = [...process.execArgv, ...process.argv.slice(1)]) {
  const quote = process.platform === "win32"
    ? value => `'${String(value).replaceAll("'", "''")}'`
    : value => `'${String(value).replaceAll("'", "'\"'\"'")}'`;
  return (process.platform === "win32" ? "& " : "") + [command, ...args].map(quote).join(" ");
}
