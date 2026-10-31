// Pi 1.1.0's native Responses client requires a credential even for a
// keyless local endpoint. Refuse that unsupported contract before dispatch;
// supplying a placeholder would qualify a different authentication path.
export default function register() {
 const error=new Error('step=admit Pi keyless Responses provider: pinned Pi 1.1.0 requires an API key; recovery=select a native Pi release with a supported keyless Responses provider before running installed-pi-recursive');
 error.code='UNSUPPORTED_NATIVE_KEYLESS_PROVIDER';
 throw error;
}
