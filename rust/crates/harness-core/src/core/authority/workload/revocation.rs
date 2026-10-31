//! Administrative revocation authenticates the target without granting its use.
use super::*;
use acyclic_auth::{
    crypto::BlockId,
    verify::{AccountState, RevocationCheck, Trust, VerifyError},
};

struct RevocationTarget<'a> {
    owner: &'a Snapshot,
    block: &'a BlockId,
}

impl Trust for RevocationTarget<'_> {
    fn environment_keys(&self, environment: &str) -> std::result::Result<&[BlockId], VerifyError> {
        self.owner.environment_keys(environment)
    }

    fn account(&self, account: &str) -> std::result::Result<AccountState<'_>, VerifyError> {
        self.owner.account(account)
    }

    fn revoked(
        &self,
        account: &str,
        check: RevocationCheck<'_>,
    ) -> std::result::Result<bool, VerifyError> {
        let revoked = self.owner.revoked(account, check)?;
        Ok(revoked && !matches!(check, RevocationCheck::Block(block) if block == self.block))
    }
}

pub(in crate::core::authority) fn revoke(root: &AuthorityRoot, scope: &Scope) -> Result<()> {
    let mut trust = root
        .runtime
        .public
        .trust
        .write()
        .map_err(|_| Error::Storage("runtime trust poisoned".into()))?;
    let target = RevocationTarget {
        owner: &trust,
        block: &scope.proof,
    };
    let timestamp = root.runtime.public.now();
    let authenticated = authenticate(&root.runtime.public, &target, scope, timestamp, &mut None)?;
    authorize_scope(
        &target,
        &root.audience,
        scope,
        timestamp,
        &authenticated,
        &mut None,
    )?;
    if trust.revoked_block_ids.contains(scope.proof.as_ref()) {
        return Ok(());
    }
    let revision = root
        .runtime
        .revision
        .borrow()
        .checked_add(1)
        .ok_or_else(|| Error::Storage("runtime trust revision overflow".into()))?;
    std::sync::Arc::make_mut(&mut trust)
        .revoked_block_ids
        .insert(*scope.proof);
    root.runtime.revision.send_replace(revision);
    Ok(())
}
