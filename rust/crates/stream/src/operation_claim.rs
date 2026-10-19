//! Process-local exclusion shared by every adapter of one provider owner.

struct Node {
    key: [u8; 48],
    next: Option<Box<Node>>,
}

/// Live operation ownership independent of persisted crash-recovery records.
#[derive(Default)]
pub struct OperationClaims(crate::query_sync::SyncMutex<Option<Box<Node>>>);

/// An operation and its caller-admitted storage, released together on drop.
pub struct OperationClaim<'a, H> {
    claims: &'a OperationClaims,
    key: [u8; 48],
    _storage: H,
}

impl OperationClaims {
    /// Visits each active claim before inspecting it, then admits the exact
    /// node allocation before construction. Returns `None` for a live duplicate.
    /// The caller retains its own accounting error type and storage custody.
    pub fn claim<H, E>(
        &self,
        key: [u8; 48],
        mut visit: impl FnMut() -> Result<(), E>,
        admit: impl FnOnce(u64) -> Result<H, E>,
    ) -> Result<Option<OperationClaim<'_, H>>, E> {
        // Admission and visit callbacks run before list mutation; unwinding
        // cannot leave a partially linked node. Retain live claims on poison.
        let mut active = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut cursor = active.as_deref();
        while let Some(node) = cursor {
            visit()?;
            if node.key == key {
                return Ok(None);
            }
            cursor = node.next.as_deref();
        }
        let storage = admit(std::mem::size_of::<Node>() as u64)?;
        let node = Box::new(Node {
            key,
            next: active.take(),
        });
        *active = Some(node);
        Ok(Some(OperationClaim {
            claims: self,
            key,
            _storage: storage,
        }))
    }
}

impl Drop for OperationClaims {
    fn drop(&mut self) {
        // A caller may forget a guard in safe Rust. Drain the remaining nodes
        // iteratively so registry destruction never recurses with claim count.
        let mut active = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while let Some(mut node) = active.take() {
            *active = node.next.take();
        }
    }
}

impl<H> Drop for OperationClaim<'_, H> {
    fn drop(&mut self) {
        let mut active = self
            .claims
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut cursor = &mut *active;
        loop {
            match cursor {
                Some(node) if node.key == self.key => {
                    *cursor = node.next.take();
                    return;
                }
                Some(node) => cursor = &mut node.next,
                None => unreachable!("live operation claim disappeared"),
            }
        }
    }
}

#[cfg(all(test, not(acyclic_loom)))]
mod tests {
    use super::*;

    #[test]
    fn panicking_admission_preserves_live_ownership_and_retirement() {
        let claims = OperationClaims::default();
        let original = claims
            .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
            .unwrap()
            .unwrap();
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = claims.claim(
                [2; 48],
                || Ok::<_, ()>(()),
                |_| -> Result<(), ()> { panic!("admission failed") },
            );
        }));
        assert!(failed.is_err());
        assert!(
            claims
                .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_none()
        );
        assert!(
            claims
                .claim([2; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );
        drop(original);
        assert!(
            claims
                .claim([1; 48], || Ok::<_, ()>(()), |_| Ok(()))
                .unwrap()
                .is_some()
        );
    }
}
