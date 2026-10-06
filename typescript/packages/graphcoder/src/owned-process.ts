/**
 * Compatibility surface for the Node host adapter.
 *
 * Process ownership belongs to the SDK filesystem host boundary. GraphCoder
 * keeps this local module only so existing Node imports remain source and
 * declaration compatible; importing it is still limited to GraphCoder's Node
 * host subpaths.
 */
export {
  defaultOwnedProcessOwner,
  retryOwnedProcessTermination,
  spawnOwnedProcess,
  terminateOwnedProcess,
} from "@acyclic-labs/fs/native-process-node";
export type {
  OwnedProcessOwner,
  OwnedProcessTermination,
} from "@acyclic-labs/fs/native-process-node";
