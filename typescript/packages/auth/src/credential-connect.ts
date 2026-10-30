import { Code, ConnectError, type Interceptor } from "@connectrpc/connect";
import { credentialStream, resolveCredential, watchCredential, type CredentialProvider, type CredentialRequest } from "./credential.js";

/** One exact credential attempt across unary and duplex Connect transports.
 * Source changes stop the original HTTP owner; they never replay the request.
 * Only an authenticated rejection invalidates the attempt's exact lease. */
export function credentialInterceptor(credential: CredentialProvider): Interceptor {
  return next => async request => {
    // Register before resolution so changes cannot fall between lease admission
    // and the transport acquiring its original request/response body owners.
    const watch = watchCredential(credential, request.signal,
      () => new ConnectError("credential source changed or unavailable", Code.PermissionDenied));
    const signal = watch?.signal ?? request.signal;
    const cleanup = () => watch?.close();
    const rejected = (error: unknown) => error instanceof ConnectError && error.code === Code.Unauthenticated;
    const failure = (error: unknown) => !rejected(error) && watch?.changed.aborted && !request.signal.aborted
      ? new ConnectError("credential source changed or unavailable", Code.PermissionDenied) : error;
    let retained = false;
    let lease: CredentialRequest | undefined;
    try {
      const admitted = await resolveCredential(credential, signal);
      lease = admitted;
      const custody = { ...admitted, check() {
        signal.throwIfAborted();
        try { admitted.check(); }
        catch { throw new ConnectError("credential source changed or expired", Code.PermissionDenied); }
      } };
      request.header.set("authorization", `Bearer ${admitted.token()}`);
      custody.check();
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
      if (rejected(error)) lease?.reject();
      throw failure(error);
    }
    finally { if (!retained) cleanup(); }
  };
}
