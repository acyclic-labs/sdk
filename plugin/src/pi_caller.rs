//! Native caller authentication using the original coordinator's signed scope.
//! Caller proofs authorize RPC identity only, never task admission or file IO.

use super::*;
use acyclic_harness::{Capabilities, TaskId, core::Scope};

pub(crate) const CALLER_ENVIRONMENT: &str = "ACYCLIC_PI_CALLER";
const CALLER_CAPABILITY: &str = "pi:caller";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiCallerClaims {
    version: u32,
    pub(crate) root_session: String,
    pub(crate) native_session: String,
    pub(crate) task: TaskId,
}

pub(crate) fn routing_session(scope: &Scope) -> Result<String, String> {
    if scope.id().len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err("Pi caller claims exceed their byte bound".to_owned());
    }
    let claims: PiCallerClaims = serde_json::from_str(scope.id()).map_err(display)?;
    if claims.version != 1 || claims.root_session.is_empty() || claims.native_session.is_empty() {
        return Err("Pi caller claims are malformed".to_owned());
    }
    // Unverified routing chooses an existing actor, never grants access. That
    // actor verifies the original protected issuer before opening any resource.
    Ok(claims.root_session)
}

pub(crate) fn issue(
    owner: &pi_owner::PiOwnerConfig, root_session: &str, binding: &PiTaskBinding,
) -> Result<Scope, String> {
    if root_session.is_empty() || binding.native_session_id.is_empty() {
        return Err("Pi caller has no original session".to_owned());
    }
    let claims = PiCallerClaims { version: 1, root_session: root_session.to_owned(),
        native_session: binding.native_session_id.clone(), task: binding.task_id };
    let identity = serde_json::to_string(&claims).map_err(display)?;
    if identity.len() > MAXIMUM_CONTROL_MESSAGE_BYTES {
        return Err("Pi caller claims exceed their byte bound".to_owned());
    }
    Ok(owner.authority()?.issuer.root_for_agent(binding.agent_id, identity,
        Capabilities::new([CALLER_CAPABILITY])))
}

#[derive(Serialize)]
struct NativeCallerClaims<'a> {
    version: u32,
    root_session: &'a str,
    native_session: &'a str,
    task: TaskId,
}

pub(crate) fn issue_with_work(
    owner: &pi_owner::PiOwnerConfig, root_session: &str, binding: &PiTaskBinding,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(Scope, acyclic_fs::NativeAllocation)> {
    work.admit(acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
    if root_session.is_empty() || binding.native_session_id.is_empty() {
        return Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi caller has no original session",
        });
    }
    let authority = owner.authority_with_native_work(work)?;
    let identity = pi_root_provider::guarded_pi_bytes(&NativeCallerClaims {
        version: 1, root_session, native_session: &binding.native_session_id,
        task: binding.task_id,
    }, MAXIMUM_CONTROL_MESSAGE_BYTES as u64, work)?;
    let identity_text = match std::str::from_utf8(&identity.0) {
        Ok(text) => text,
        Err(_) => {
            drop(identity.0);
            drop(identity.1);
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi caller claims have invalid UTF-8",
            });
        }
    };
    let capabilities = Capabilities::new_with_native_work([CALLER_CAPABILITY], work)?;
    let scope = authority.0.issuer.root_for_agent_with_native_work(
        binding.agent_id, identity_text, &capabilities.0, work,
    )?;
    drop(identity.0);
    drop(identity.1);
    drop(capabilities.0);
    drop(capabilities.1);
    drop(authority.0);
    drop(authority.1);
    Ok(scope)
}

/// Issues the original coordinator proof without copying claims or encoding an
/// unfunded native environment value. The returned custody covers both strings.
pub(crate) fn issued_environment_with_work(
    owner: &pi_owner::PiOwnerConfig, root_session: &str, binding: &PiTaskBinding,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<(BTreeMap<String, String>, acyclic_fs::NativeAllocation)> {
    let scope = issue_with_work(owner, root_session, binding, work)?;
    let mut custody;
    let encoded;
    (encoded, custody) = pi_root_provider::guarded_pi_bytes(
        &scope.0, MAXIMUM_CONTROL_MESSAGE_BYTES as u64, work,
    )?;
    drop(scope.0);
    drop(scope.1);
    let encoded = match String::from_utf8(encoded) {
        Ok(value) => value,
        Err(error) => {
            drop(error);
            drop(custody);
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi caller scope has invalid UTF-8",
            });
        }
    };
    let mut key_custody;
    let key;
    (key, key_custody) = guarded_cli_text(&[CALLER_ENVIRONMENT], work)?;
    if let Err(cause) = custody.absorb(&mut key_custody) {
        drop(key);
        drop(encoded);
        return Err(acyclic_harness::Error::NativeWork { cause, custody: Some(custody) });
    }
    Ok((BTreeMap::from([(key, encoded)]), custody))
}

pub(crate) fn verify_with_work<'a>(
    owner: &pi_owner::PiOwnerConfig, state: &'a AdapterState, scope: &Scope,
    native_session: &str, work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<&'a PiTaskBinding> {
    let denied = |reason| acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason,
    };
    let authority = owner.authority_with_native_work(work)?;
    let verified = authority.0.verifier.verify_with_native_work(scope, work);
    drop(authority.0);
    drop(authority.1);
    verified?;
    let capabilities = Capabilities::new_with_native_work([CALLER_CAPABILITY], work)?;
    let caller_only = scope.capabilities() == &capabilities.0;
    drop(capabilities.0);
    drop(capabilities.1);
    if !caller_only {
        return Err(denied("Pi caller scope carries non-caller authority"));
    }
    let binding = match &state.task_binding {
        PluginTaskBinding::Pi(binding) if binding.native_session_id == native_session =>
            Some(binding.as_ref()),
        _ => state.routes.values().find_map(|route| match &route.task_binding {
            PluginTaskBinding::Pi(binding) if binding.native_session_id == native_session =>
                Some(binding.as_ref()),
            _ => None,
        }),
    }.ok_or_else(|| denied("Pi caller task has no original admitted route"))?;
    if scope.agent() != Some(binding.agent_id) {
        return Err(denied("Pi caller task, agent or native session was substituted"));
    }
    // Compare the issuer's exact borrowed claims projection without making a
    // second owned claims DTO from the authenticated scope's identity string.
    let identity = pi_root_provider::guarded_pi_bytes(&NativeCallerClaims {
        version: 1, root_session: &state.root_session_id, native_session,
        task: binding.task_id,
    }, MAXIMUM_CONTROL_MESSAGE_BYTES as u64, work)?;
    let original = scope.id().as_bytes() == identity.0.as_slice();
    drop(identity.0);
    drop(identity.1);
    if !original {
        return Err(denied("Pi caller differs from its original native session"));
    }
    Ok(binding)
}

pub(crate) fn verify<'a>(
    owner: &pi_owner::PiOwnerConfig, state: &'a AdapterState, scope: &Scope,
    native_session: &str,
) -> Result<&'a PiTaskBinding, String> {
    owner.authority()?.verifier.verify(scope).map_err(display)?;
    if scope.capabilities() != &Capabilities::new([CALLER_CAPABILITY]) {
        return Err("Pi caller scope carries non-caller authority".to_owned());
    }
    let claims: PiCallerClaims = serde_json::from_str(scope.id()).map_err(display)?;
    if claims.version != 1 || claims.root_session != state.root_session_id
        || claims.native_session != native_session
    {
        return Err("Pi caller differs from its original native session".to_owned());
    }
    let binding = match &state.task_binding {
        PluginTaskBinding::Pi(binding) if binding.task_id == claims.task => binding.as_ref(),
        _ => state.routes.values().find_map(|route| match &route.task_binding {
            PluginTaskBinding::Pi(binding) if binding.task_id == claims.task => Some(binding.as_ref()),
            _ => None,
        }).ok_or_else(|| "Pi caller task has no original admitted route".to_owned())?,
    };
    if binding.native_session_id != native_session || scope.agent() != Some(binding.agent_id) {
        return Err("Pi caller task, agent or native session was substituted".to_owned());
    }
    Ok(binding)
}

#[cfg(test)]
#[path = "pi_caller_tests.rs"]
mod tests;
