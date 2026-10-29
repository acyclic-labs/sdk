import type { GuestAdmissionKind } from "./contract.js";
export type { GuestAdmissionKind } from "./contract.js";

/** Original host-side admission port. Its Rust GuestAdmission owner authenticates
 * once before guest entry and checks current trust/expiry/stop at each projection.
 * Never obtain this port from guest env, request data or a deployment descriptor.
 * Both values belong to that SAME invocation and retain its real trust source.
 * The port itself stays outside the guest; only its selected value is exposed.
 */
export interface GuestAdmissionHost<PlatformAuthority, HostAccess> {
  binding(kind: "platformAuthority"): PlatformAuthority;
  binding(kind: "hostAccess"): HostAccess;
}

/** Materialize one Rust-validated binding selection, on native or hosted JS.
 * Absence refuses; resource grants and identity aliases cannot supply admission.
 * Reuse the original port/values across selections, without minting, copying
 * credentials, creating a trust watcher or starting any asynchronous work.
 */
export function materializeGuestAdmission<A, H>(kind: "platformAuthority", host: GuestAdmissionHost<A, H> | undefined): A;
export function materializeGuestAdmission<A, H>(kind: "hostAccess", host: GuestAdmissionHost<A, H> | undefined): H;
export function materializeGuestAdmission<A, H>(kind: GuestAdmissionKind, host: GuestAdmissionHost<A, H> | undefined): A | H;
export function materializeGuestAdmission<A, H>(
  kind: GuestAdmissionKind,
  host: GuestAdmissionHost<A, H> | undefined,
): A | H {
  if (host === undefined || host === null || typeof host.binding !== "function")
    throw new TypeError("guest admission missing; recovery: supply the authenticated invocation's admission binding before guest entry");
  switch (kind) {
    case "platformAuthority": return host.binding(kind);
    case "hostAccess": return host.binding(kind);
    default: throw new TypeError("guest admission kind invalid; recovery: validate the deployment manifest with the Rust manifest validator");
  }
}
