//! Portable cases for fresh, disposable scopes. Cloud supplies transports and
//! credentials; neither account selection nor an authority bypass is available.
use super::{Client, Error, Transport};
use crate::contract::*;

/// Canonical language-neutral issue inventory, not a captured Cloud response.
pub const SUITE: &[u8] = include_bytes!("../../conformance/secrets-v1.json");

fn list(scope: &str, cursor: Option<SecretListCursor>, page_size: u32) -> SecretListRequest {
    SecretListRequest {
        scope: scope.into(),
        cursor,
        page_size,
    }
}

/// Verify the real provider's CAS, visibility, paging fence and grants.
/// `allowed` needs `secrets/put`, `secrets/list` and `secrets/delete` on
/// `secrets:/<scope>`; `denied` must carry existing product grants and no
/// secrets grant. Every failure is fixed text so a failing provider cannot
/// expose protected values.
pub async fn verify<T: Transport, U: Transport>(
    allowed: &Client<T>,
    denied: &Client<U>,
    scope: &str,
) -> Result<(), &'static str> {
    let initial = allowed
        .list(&list(scope, None, 0))
        .await
        .map_err(|_| "secrets/list initial failed")?;
    if !initial.variables.is_empty() || initial.next_cursor.is_some() {
        return Err("secrets conformance requires a fresh scope");
    }
    let protected = SecretPutRequest {
        scope: scope.into(),
        name: "protected".into(),
        kind: SecretKind::Protected,
        value: "conformance-protected-value-never-return".into(),
        expected_generation: initial.generation,
    };
    let first = allowed
        .put(&protected)
        .await
        .map_err(|_| "secrets/put protected failed")?;
    let plain = SecretPutRequest {
        scope: scope.into(),
        name: "plain".into(),
        kind: SecretKind::Plain,
        value: "conformance-plain-value".into(),
        expected_generation: first.generation,
    };
    let second = allowed
        .put(&plain)
        .await
        .map_err(|_| "secrets/put plain failed")?;

    // Paging: one row per page reaches every row in name order, all from one
    // generation, and ends with a null cursor.
    let page = allowed
        .list(&list(scope, None, 1))
        .await
        .map_err(|_| "secrets/list first page failed")?;
    let plain_row = page.variables.iter().any(|row| matches!(&row.kind,
        SecretVariableKind::Plain(row) if row.name == plain.name && row.value == plain.value && row.version == second.version));
    let Some(cursor) = page.next_cursor.clone() else {
        return Err("secrets/list did not page at the requested chunk");
    };
    if page.generation != second.generation || page.variables.len() != 1 || !plain_row {
        return Err("secrets/list visibility or generation diverged");
    }
    let last = allowed
        .list(&list(scope, Some(cursor.clone()), 1))
        .await
        .map_err(|_| "secrets/list continuation failed")?;
    let protected_row = last.variables.iter().any(|row| matches!(&row.kind,
        SecretVariableKind::Protected(row) if row.name == protected.name && row.version == first.version));
    if last.generation != second.generation || last.variables.len() != 1 || !protected_row {
        return Err("secrets/list visibility or generation diverged");
    }
    if let Some(end) = last.next_cursor {
        let empty = allowed
            .list(&list(scope, Some(end), 1))
            .await
            .map_err(|_| "secrets/list continuation failed")?;
        if !empty.variables.is_empty() || empty.next_cursor.is_some() {
            return Err("secrets/list returned rows past the last name");
        }
    }

    let deletion = SecretDeleteRequest {
        scope: scope.into(),
        name: protected.name.clone(),
        expected_generation: initial.generation,
    };
    if allowed.put(&protected).await != Err(Error::Conflict)
        || allowed.delete(&deletion).await != Err(Error::Conflict)
    {
        return Err("secrets stale generation did not conflict");
    }
    let mut deletion = deletion;
    deletion.expected_generation = second.generation;
    let mut denied_put = protected.clone();
    denied_put.expected_generation = second.generation;
    if denied.put(&denied_put).await != Err(Error::PermissionDenied)
        || denied.list(&list(scope, None, 0)).await != Err(Error::PermissionDenied)
        || denied.delete(&deletion).await != Err(Error::PermissionDenied)
    {
        return Err("secrets existing product grants implied secrets authority");
    }
    let third = allowed
        .delete(&deletion)
        .await
        .map_err(|_| "secrets/delete protected failed")?;

    // The fence: a mutation after the first page ends that listing typed.
    if allowed.list(&list(scope, Some(cursor), 1)).await != Err(Error::ListingChanged) {
        return Err("secrets/list continued a listing across a mutation");
    }

    let fourth = allowed
        .delete(&SecretDeleteRequest {
            scope: scope.into(),
            name: plain.name,
            expected_generation: third.generation,
        })
        .await
        .map_err(|_| "secrets/delete plain failed")?;
    let remaining = allowed
        .list(&list(scope, None, 0))
        .await
        .map_err(|_| "secrets/list after deletion failed")?;
    if remaining.generation != fourth.generation
        || !remaining.variables.is_empty()
        || remaining.next_cursor.is_some()
    {
        return Err("secrets/delete receipts did not match the stored scope");
    }
    Ok(())
}
