import { resolveClientConnection } from "../generated/wasm/acyclic_harness_wasm.js";
export type { ClientConnectionError } from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureConnectionWasm as initializeClientConnection } from "./wasm-runtime.js";

/** Native hosts read these same names. Browsers can supply the explicit inputs. */
export interface ClientEnvironment {
  readonly [name: string]: string | undefined;
  readonly ACYCLIC_ENDPOINT?: string;
  readonly ACYCLIC_API_KEY?: string;
}

export interface ClientConnectionOptions {
  readonly endpoint: string;
  readonly credential: string;
}

/** After initializeClientConnection(), resolve without I/O. Precedence, the managed
 * default and all validation live in Rust. This does not bootstrap a session
 * capability or discover/create an agent. */
export function resolveConnectionOptions(endpoint?: string, credential?: string,
  environment: ClientEnvironment = typeof process === "undefined" ? {} : process.env): ClientConnectionOptions {
  const options = resolveClientConnection(endpoint, credential, environment.ACYCLIC_ENDPOINT, environment.ACYCLIC_API_KEY);
  try {
    return {endpoint: options.endpoint(), credential: options.credential()};
  } finally {
    options.free();
  }
}
