use super::*;
use acyclic_fs::access::Action;

/// An import retains authenticated endpoints; its SDK plan cannot escape.
///
/// ```compile_fail
/// use acyclic_harness::filesystem::VolumeImportPlan;
/// fn escape<A, O>(plan: VolumeImportPlan<'_, A, O>) -> acyclic_fs::JoinPlan<A, O> {
///     plan.plan
/// }
/// ```
pub struct VolumeImportPlan<'a, A, O> {
    plan: JoinPlan<A, O>,
    source_grant: &'a ContentGrant,
    target_grant: &'a ContentGrant,
}

impl<'a, A, O> VolumeImportPlan<'a, A, O> {
    pub(super) fn new(
        plan: JoinPlan<A, O>,
        source_grant: &'a ContentGrant,
        target_grant: &'a ContentGrant,
    ) -> Self {
        Self {
            plan,
            source_grant,
            target_grant,
        }
    }

    /// Destination generation used by this plan's publication CAS.
    pub fn target_head(&self) -> GenerationId {
        self.plan.target_head()
    }
    /// Immutable source selected during preparation.
    pub fn source_head(&self) -> GenerationId {
        self.plan.source_head()
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore> VolumeImportPlan<'_, A, O> {
    /// Reauthorize at application and hold source policy stable through publication.
    pub async fn apply(
        &self,
        options: acyclic_fs::ApplyOptions,
        resolutions: BTreeMap<
            acyclic_fs::MergeConflict,
            acyclic_fs::kernel::MergeConflictResolution,
        >,
        permit: acyclic_fs::PublicationPermit,
    ) -> Result<acyclic_fs::JoinOutcome<A, O>> {
        self.plan
            .source()
            .with_stable_policy(|| async {
                let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                let source = super::volume_access::compile(
                    self.plan.source(),
                    self.source_grant,
                    Some(self.plan.source_head()),
                    VolumeOperation::Read,
                    &[Action::Read, Action::List],
                    &io,
                )
                .await?;
                let target = super::volume_access::compile(
                    self.plan.target(),
                    self.target_grant,
                    None,
                    VolumeOperation::Write,
                    &[Action::MergeInto],
                    &io,
                )
                .await?;
                super::super::access::require_current(self.plan.source(), &source.0, &io).await?;
                super::super::access::require_current(self.plan.target(), &target.0, &io).await?;
                drop((source, target));
                self.plan
                    .apply(options, resolutions, permit)
                    .await
                    .map_err(map_error)
            })
            .await
            .map_err(map_error)?
    }
}
