//! Per-call resolution of a `shell` command's execution target.
use crate::{
    Error, Result,
    agent::{Roster, TargetRecord},
    tool::ToolExecutor,
};
use std::sync::Arc;

/// One resolved target: its record and the executor that runs its commands.
pub struct Target {
    /// The target as the roster lists it.
    pub record: TargetRecord,
    /// Runs the target's `shell` (and, on the home target, `fs` and hooks).
    pub executor: Arc<dyn ToolExecutor>,
}

/// Resolves each call's target. One resolver serves an agent's whole turn;
/// it never falls back from the named target to another one.
pub trait TargetResolver: acyclic_stream::ProviderPlatform {
    /// Every target visible to the agent now, and its home.
    fn roster(&self) -> Arc<Roster>;

    /// The target `target` names ([`Roster::select`]); its executor.
    ///
    /// # Errors
    /// [`Error::Target`] when it is unknown or offline.
    fn resolve(&self, target: Option<&str>) -> Result<Arc<Target>>;
}

/// A tree whose every target runs in this process: today the home target,
/// a local checkout or a bound Machines workspace.
pub struct InProcessTargets {
    roster: Arc<Roster>,
    targets: Vec<Arc<Target>>,
}

impl InProcessTargets {
    /// The one home target `record`, run by `executor`.
    ///
    /// # Errors
    /// When the record is malformed.
    pub fn home(record: TargetRecord, executor: Arc<dyn ToolExecutor>) -> Result<Self> {
        let home = record.name.clone();
        Self::new(&home, vec![Target { record, executor }])
    }

    /// `targets`, whose home is `home`: for example the agent's checkout and
    /// its own Machine on each provisioned target
    /// (`AgentMachines::executor`, with the `machines` feature).
    ///
    /// # Errors
    /// When a record is malformed, a name repeats or the home is unlisted.
    pub fn new(home: &str, targets: Vec<Target>) -> Result<Self> {
        let records = targets.iter().map(|target| target.record.clone()).collect();
        let roster = Roster::new(home, records).map_err(agent_error)?;
        Ok(Self {
            roster: Arc::new(roster),
            targets: targets.into_iter().map(Arc::new).collect(),
        })
    }

    /// The home record of a checkout on this host, owned by `owner`.
    #[must_use]
    pub fn local_record(name: &str, owner: &str) -> TargetRecord {
        Self::record(
            (name, owner),
            (
                crate::agent::TargetClass::Attached,
                crate::agent::DeviceKind::Desktop,
            ),
            std::env::consts::OS,
            &crate::tools::shell::PLATFORM,
        )
    }

    /// The online record of target `name` owned by `owner`, of `class`
    /// and `device_kind`, running `shell` on `os`.
    #[must_use]
    pub fn record(
        (name, owner): (&str, &str),
        (class, device_kind): (crate::agent::TargetClass, crate::agent::DeviceKind),
        os: &str,
        shell: &acyclic_host_platform::shell::ShellSpec,
    ) -> TargetRecord {
        TargetRecord {
            name: name.to_owned(),
            owner: owner.to_owned(),
            class,
            os: os.to_owned(),
            shell: shell.language.to_owned(),
            path_style: crate::agent::PathStyle::of(shell),
            device_kind,
            state: crate::agent::TargetState::Online,
        }
    }
}

impl TargetResolver for InProcessTargets {
    fn roster(&self) -> Arc<Roster> {
        Arc::clone(&self.roster)
    }

    fn resolve(&self, target: Option<&str>) -> Result<Arc<Target>> {
        let record = self.roster.select(target).map_err(Error::Target)?;
        self.targets
            .iter()
            .find(|target| target.record.name == record.name)
            .cloned()
            .ok_or_else(|| Error::Invalid("listed target has no executor".into()))
    }
}

fn agent_error(error: crate::agent::Error) -> Error {
    Error::Invalid(error.to_string())
}

/// The target a workspace tool call runs on: the one a `shell` call names,
/// its home when omitted; `fs` always runs on the home target. `None` for a
/// call of any other tool.
///
/// # Errors
/// [`Error::Target`] when the named target cannot take the call.
pub(crate) fn call_target(
    targets: &dyn TargetResolver,
    invocation: &crate::tool::ToolInvocation,
) -> Option<Result<Arc<Target>>> {
    let workspace = crate::tools::catalog().any(|spec| {
        spec.name == invocation.name && spec.binding == crate::tools::ToolBinding::Workspace
    });
    if !workspace {
        return None;
    }
    let named = (invocation.name == crate::tools::shell::PLATFORM.name)
        .then(|| invocation.arguments.get("target"))
        .flatten()
        .and_then(serde_json::Value::as_str);
    Some(targets.resolve(named))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Unused;
    impl ToolExecutor for Unused {
        fn execute<'a>(
            &'a self,
            _: crate::tool::ToolInvocation,
            _: Option<super::super::ToolContext>,
        ) -> acyclic_stream::BoxProviderFuture<'a, Result<crate::tool::ToolResult>> {
            Box::pin(async { Err(Error::Unsupported("unused".into())) })
        }
        fn reconcile<'a>(
            &'a self,
            _: crate::tool::ToolInvocation,
            _: Option<super::super::ToolContext>,
        ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<crate::tool::ToolResult>>>
        {
            Box::pin(async { Ok(None) })
        }
    }

    #[test]
    fn the_home_target_resolves_by_omission_name_and_owner_only() -> Result<()> {
        let targets = InProcessTargets::home(
            InProcessTargets::local_record("local", "ana"),
            Arc::new(Unused),
        )?;
        for named in [None, Some("local"), Some("user:ana")] {
            assert_eq!(targets.resolve(named)?.record.name, "local");
        }
        // Negative control: another name or user is refused, never served by home.
        for (named, text) in [
            ("pc", "target 'pc' is unknown; online: local"),
            ("user:bo", "target 'user:bo' is unknown; online: local"),
        ] {
            let refused = targets.resolve(Some(named)).err().expect("refused");
            assert_eq!(refused.to_string(), text);
            assert!(!refused.uncertain());
        }
        Ok(())
    }
}
