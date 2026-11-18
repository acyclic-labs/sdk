import { Code, ConnectError, type Interceptor } from "@connectrpc/connect";
import { credentialStream, resolveCredential, watchCredential, type CredentialProvider, type CredentialRequest } from "./credential.js";

class LocalCredentialError extends ConnectError {
  static override [Symbol.hasInstance](value: unknown): boolean {
    return Function.prototype[Symbol.hasInstance].call(this, value);
  }
}

/** Credential admission failed before the transport acquired the request. */
export class RequestCredentialError extends LocalCredentialError {
  constructor(cause: unknown) { super("credential source unavailable", Code.Unavailable, undefined, undefined, cause); }
}

/** Local credential refusal after the transport acquired the request. */
export class ResponseCredentialError extends LocalCredentialError {
  constructor(message: string) { super(message, Code.PermissionDenied); }
}

class CredentialAdmissionError extends LocalCredentialError {
  constructor(message: string) { super(message, Code.PermissionDenied); }
}

/** One exact credential attempt across unary and duplex Connect transports.
 * Source changes stop the original HTTP owner; they never replay the request.
 * Only an authenticated rejection invalidates the attempt's exact lease. */
export function credentialInterceptor(credential: CredentialProvider): Interceptor {
  return next => async request => {
    // Register before resolution so changes cannot fall between lease admission
    // and the transport acquiring its original request/response body owners.
    let watch;
    try {
      watch = watchCredential(credential, request.signal,
        () => new ConnectError("credential source changed or unavailable", Code.PermissionDenied));
    } catch (error) {
      throw request.signal.aborted ? error : new RequestCredentialError(error);
    }
    const signal = watch?.signal ?? request.signal;
    const cleanup = () => watch?.close();
    const rejected = (error: unknown) => error instanceof ConnectError && error.code === Code.Unauthenticated;
    let dispatched = false;
    const failure = (error: unknown) => {
      if (!dispatched && !request.signal.aborted) return new RequestCredentialError(error);
      if (rejected(error)) return error;
      const changed = watch?.changed.aborted && !request.signal.aborted;
      if (!changed && !(error instanceof CredentialAdmissionError)) return error;
      const message = error instanceof CredentialAdmissionError ? error.rawMessage : "credential source changed or unavailable";
      return dispatched ? new ResponseCredentialError(message) : new ConnectError(message, Code.PermissionDenied);
    };
    let retained = false;
    let lease: CredentialRequest | undefined;
    try {
      const admitted = await resolveCredential(credential, signal);
      lease = admitted;
      const custody = { ...admitted, check() {
        signal.throwIfAborted();
        try { admitted.check(); }
        catch { throw new CredentialAdmissionError("credential source changed or expired"); }
      } };
      request.header.set("authorization", `Bearer ${admitted.token()}`);
      custody.check();
      dispatched = true;
      const response = await next(request.stream
        ? { ...request, signal, message: credentialStream(request.message, custody, () => false) }
        : { ...request, signal });
      custody.check();
      if (!response.stream) return response;
      const message = async function* () {
        try { yield* credentialStream(response.message, custody, rejected); }
        catch (error) { throw failure(error); }
        finally { cleanup(); }
      };
      retained = true;
      return { ...response, message: message() };
    }
    catch (error) {
      if (dispatched && rejected(error)) lease?.reject();
      throw failure(error);
    }
    finally { if (!retained) cleanup(); }
  };
}
