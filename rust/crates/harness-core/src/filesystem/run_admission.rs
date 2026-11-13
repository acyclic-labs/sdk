//! Select operator inputs from the original protected file-publication receipt.
use super::*;
use crate::{
    OperationId,
    conversation::{ContentAccess, ContentPublisher},
    supervision::{RunAdmission, RunAdmissionSource},
};

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// One protected operation lookup and three original file reads, without a
    /// volume/history scan. A missing receipt never falls back to a writable
    /// lookalike declaration. The shared turn owner and client use this path.
    pub(in crate::filesystem) async fn original_run_admission(
        self: &Arc<Self>,
        volume: &VolumeRef,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation: OperationId,
        maximum_bytes: Option<u64>,
    ) -> Result<Option<RunAdmissionSource>> {
        let publisher = FilesystemContentPublisher::new(
            self.clone(),
            volume,
            verifier,
            scope,
            maximum_bytes,
            None,
        )?;
        let (receipt, _hold) = publisher
            .observe_changes(
                volume,
                operation,
                maximum_bytes,
                ContentAccess {
                    ceiling: Some(scope.capabilities()),
                    io: None,
                },
            )?
            .await?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        if receipt.members.len() != 3 {
            return Err(Error::Invalid(
                "run admission is not an original input batch".into(),
            ));
        }
        let select = |path: &str| {
            receipt
                .files()
                .find(|file| file.path() == path)
                .cloned()
                .ok_or_else(|| Error::Invalid("run admission omits an original input".into()))
        };
        let admission = select(&format!("runs/{operation}.json"))?;
        let goal = select("GOAL.md")?;
        let policy = select("control/supervision.json")?;
        let grant = ContentGrant::verify_directory_read(verifier, scope, volume, "", &mut None)?;
        let (declaration, original_goal, original_policy) = futures::try_join!(
            self.read_content(&admission, &grant, maximum_bytes, None),
            self.read_content(&goal, &grant, maximum_bytes, None),
            self.read_content(&policy, &grant, maximum_bytes, None),
        )?;
        let declaration: RunAdmission = serde_json::from_slice(&declaration)
            .map_err(|_| Error::Invalid("invalid original run admission".into()))?;
        let value = declaration.verify(operation, &original_goal, &original_policy)?;
        Ok(Some(RunAdmissionSource {
            admission,
            goal,
            policy,
            value,
        }))
    }
}
