//! Administrative revocation authenticates the target without granting its use.
use super::*;
use acyclic_auth::{
    crypto::BlockId,
    verify::{AccountState, Keys, RevocationCheck, Trust, VerifyError},
};

struct RevocationTarget<'a> {
    owner: &'a dyn Trust,
    block: &'a BlockId,
}

impl Trust for RevocationTarget<'_> {
    fn environment_keys(&self, environment: &str) -> std::result::Result<Keys<'_>, VerifyError> {
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
    // The original signing lock serializes authentication, duplicate detection
    // and publication, so concurrent repeats publish only once.
    let mut keys = root.runtime.keys()?;
    let trust = &*root.runtime.trust;
    let target = RevocationTarget {
        owner: trust,
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
    if trust
        .revoked(&scope.issuer, RevocationCheck::Block(&scope.proof))
        .map_err(unauthorized)?
    {
        return Ok(());
    }
    keys.revoke(
        trust,
        *scope.proof,
        authenticated
            .capability
            .effective()
            .expires_at_unix_seconds(),
        timestamp,
    )
}
