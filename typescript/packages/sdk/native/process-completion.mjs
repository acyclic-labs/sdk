/** Decode only the original native host's bound v3 terminal. A missing root
 * or incomplete retirement remains a refusal, never a successful exit. */
export function decodeCompletion(bytes, invocation, hostStatus) {
  const invalid = cause => Object.assign(new Error(
    'step=process-completion; recovery=retain the original host receipt and invocation identity', {cause}),
    {code: 'PROCESS_COMPLETION_INVALID'});
  let value;
  try { value = JSON.parse(bytes); } catch (cause) { throw invalid(cause); }
  const integer = value => Number.isSafeInteger(value);
  const root = value?.root_status;
  if (!value || value.schema !== 'acyclic.process-custody-completion.v3'
    || value.invocation !== invocation || typeof value.platform !== 'string'
    || !integer(value.host_exit_code) || value.host_exit_code < 0 || value.host_exit_code > 0xffffffff
    || typeof value.cancelled !== 'boolean'
    || typeof value.process_tree_retired !== 'boolean'
    || (root !== null && (!root || !integer(root.native)
      || (root.code !== null && !integer(root.code))
      || (root.signal !== null && (!integer(root.signal) || root.signal <= 0))
      || ((root.code === null) === (root.signal === null))))
    || (value.process_tree_retired && !root)
    || (!value.process_tree_retired && value.failure === null)
    || (value.failure !== null && (!value.failure || typeof value.failure.phase !== 'string'
      || typeof value.failure.error?.kind !== 'string'
      || (value.failure.error.raw_os_error !== null && !integer(value.failure.error.raw_os_error))))) {
    throw invalid();
  }
  if (hostStatus !== undefined && (hostStatus.signal !== null
    || !Number.isSafeInteger(hostStatus.code)
    || hostStatus.code < -0x80000000 || hostStatus.code > 0xffffffff
    || (hostStatus.code >>> 0) !== value.host_exit_code)) {
    throw Object.assign(new Error(
      'step=process-host-close; recovery=retain the original host exit and completion receipt'),
      {code: 'PROCESS_HOST_STATUS_MISMATCH', hostStatus, completion: value});
  }
  return value;
}
