import assert from 'node:assert/strict';

// NativeSession reads the identifier derived from the custodied native event
// stream. Harness task identity and retained files cannot substitute for it.
export function nativeSessionIdentity(session) {
 assert.equal(typeof session?.id,'string','native session identifier is missing');
 assert.ok(session.id.length&&!/[\u0000-\u001f\u007f]/.test(session.id),'native session identifier is invalid');
 return session.id;
}

export function assertNativeResume(original,resumed) {
 assert.equal(nativeSessionIdentity(resumed),nativeSessionIdentity(original),'resume changed native session identity');
}
