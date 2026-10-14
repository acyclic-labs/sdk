//! Native caller authentication through the coordinator's signed scope.
use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{Capabilities, TaskId, core::Scope, filesystem::PreparationIo};

pub(crate) const CALLER_ENVIRONMENT: &str = "ACYCLIC_PI_CALLER";
const CALLER_CAPABILITY: &str = "pi:caller";

pub(crate) fn is_environment_key(key: &str) -> bool {
    [
        CALLER_ENVIRONMENT,
        "ACYCLIC_PI_ROUTE",
        "ACYCLIC_PI_TASK",
        "ACYCLIC_PI_TOKEN",
        "ACYCLIC_PI_SCOPE",
        "ACYCLIC_PI_LEASE",
    ]
    .iter()
    .any(|reserved| key.eq_ignore_ascii_case(reserved))
        || key
            .get(.."ACYCLIC_PI_CALLER_".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ACYCLIC_PI_CALLER_"))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiCallerClaims<S = String> {
    version: u32,
    pub(crate) root_session: S,
    pub(crate) native_session: S,
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
    Ok(claims.root_session)
}

fn identity(
    root_session: &str,
    native_session: &str,
    task: TaskId,
    work: &NativeMaterializationWork,
) -> acyclic_harness::Result<(Vec<u8>, NativeAllocation)> {
    pi_root_provider::guarded_pi_bytes(
        &PiCallerClaims {
            version: 1,
            root_session,
            native_session,
            task,
        },
        MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
        work,
    )
}

pub(crate) fn issue(
    owner: &pi_owner::PiOwnerConfig,
    root_session: &str,
    binding: &PiTaskBinding,
    work: &NativeMaterializationWork,
) -> acyclic_harness::Result<(Scope, NativeAllocation)> {
    work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
    if root_session.is_empty() || binding.native_session_id.is_empty() {
        return Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi caller has no original session",
        });
    }
    let authority = owner.authority(work)?;
    let identity = identity(
        root_session,
        &binding.native_session_id,
        binding.task_id,
        work,
    )?;
    let capabilities = Capabilities::copy([CALLER_CAPABILITY], work)?;
    let mut io = acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
    let scope = authority.issuer.issue(
        std::str::from_utf8(&identity.0).expect("JSON encoder emits UTF-8"),
        &capabilities.0,
        Some(binding.agent_id),
        None,
        &mut Some(&mut io),
    )?;
    Ok((scope, io.into_custody()?))
}

/// Encodes the signed caller scope for the native child's environment.
pub(crate) fn issued_environment(
    owner: &pi_owner::PiOwnerConfig,
    root_session: &str,
    binding: &PiTaskBinding,
    work: &NativeMaterializationWork,
) -> acyclic_harness::Result<(BTreeMap<String, String>, NativeAllocation)> {
    let scope = issue(owner, root_session, binding, work)?;
    let (encoded, mut custody) =
        pi_root_provider::guarded_pi_bytes(&scope.0, MAXIMUM_CONTROL_MESSAGE_BYTES as u64, work)?;
    drop(scope);
    let encoded = String::from_utf8(encoded).expect("JSON encoder emits UTF-8");
    let (key, mut key_custody) = guarded_cli_text(&[CALLER_ENVIRONMENT], work)?;
    custody.absorb(&mut key_custody)?;
    Ok((BTreeMap::from([(key, encoded)]), custody))
}

pub(crate) fn verify<'a>(
    owner: &pi_owner::PiOwnerConfig,
    state: &'a AdapterState,
    scope: &Scope,
    native_session: &str,
    work: &NativeMaterializationWork,
) -> acyclic_harness::Result<&'a PiTaskBinding> {
    let denied = |reason| acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    };
    let authority = owner.authority(work)?;
    let verified = authority.verifier.verify(
        scope,
        &mut Some(&mut &PreparationIo::from_native_work(work)?),
    );
    drop(authority);
    verified?;
    if !scope.capabilities().iter().eq([CALLER_CAPABILITY]) {
        return Err(denied("Pi caller scope carries non-caller authority"));
    }
    let binding = std::iter::once(&state.task_binding)
        .chain(state.routes.values().map(|route| &route.task_binding))
        .find_map(|binding| match binding {
            PluginTaskBinding::Pi(binding) if binding.native_session_id == native_session => {
                Some(binding.as_ref())
            }
            _ => None,
        })
        .ok_or_else(|| denied("Pi caller task has no original admitted route"))?;
    if scope.agent() != Some(binding.agent_id) {
        return Err(denied(
            "Pi caller task, agent or native session was substituted",
        ));
    }
    let identity = identity(
        &state.root_session_id,
        native_session,
        binding.task_id,
        work,
    )?;
    if scope.id().as_bytes() != identity.0.as_slice() {
        return Err(denied("Pi caller differs from its original native session"));
    }
    Ok(binding)
}

#[cfg(test)]
#[path = "pi_caller_tests.rs"]
mod tests;
