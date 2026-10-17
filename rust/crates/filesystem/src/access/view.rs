use super::{InvalidPolicy, PathTrie};
use crate::sync::RwLock;
use std::sync::Arc;

/// Published compiled access for one authenticated mount binding.
/// The trusted policy owner installs revisions; new operations load the latest
/// view, while open handles retain their original immutable view.
pub struct AccessView {
    // Terminal lock: callers never receive a guard. Compilation, allocation,
    // destruction and backend operations run outside it; it acquires no locks.
    current: RwLock<Arc<PathTrie>>,
    #[cfg(all(not(target_arch = "wasm32"), not(acyclic_fs_loom)))]
    _custody: Option<crate::NativeAllocation>,
}

impl AccessView {
    /// Bind an already authenticated policy/capability intersection.
    #[must_use]
    pub fn new(view: PathTrie) -> Self {
        Self {
            current: RwLock::new(Arc::new(view)),
            #[cfg(all(not(target_arch = "wasm32"), not(acyclic_fs_loom)))]
            _custody: None,
        }
    }

    /// Retain the original native allocation for the view and its compiled data.
    #[cfg(all(not(target_arch = "wasm32"), not(acyclic_fs_loom)))]
    pub fn new_native(
        view: PathTrie,
        custody: crate::NativeAllocation,
    ) -> Result<Arc<Self>, crate::NativeTreeMaterializationError> {
        let header = custody.work().claim(
            (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let view = native_trie(view, custody)?;
        Ok(Arc::new(Self {
            current: RwLock::new(Arc::new(view)),
            _custody: Some(header),
        }))
    }

    /// Publish a freshly authenticated revision, retaining every pinned old view.
    #[cfg(all(not(target_arch = "wasm32"), not(acyclic_fs_loom)))]
    pub fn publish_native(
        &self,
        view: PathTrie,
        custody: crate::NativeAllocation,
    ) -> Result<(), crate::NativeTreeMaterializationError> {
        let view = native_trie(view, custody)?;
        self.publish(view)
            .map_err(|_| crate::NativeTreeMaterializationError::InvalidLayout)
    }

    /// Load the current view without allocating or consulting storage.
    pub fn current(&self) -> Result<Arc<PathTrie>, InvalidPolicy> {
        self.current
            .read()
            .map(|view| Arc::clone(&view))
            .map_err(|_| InvalidPolicy("access view publication failed"))
    }

    // Negative control: checking before the publication lock lets a stale
    // writer replace a newer revision. This is never compiled into the library.
    #[cfg(all(test, acyclic_fs_loom))]
    pub(super) fn publish_unfenced(&self, view: PathTrie) -> Result<(), InvalidPolicy> {
        let current = self.current()?;
        if !can_replace(&current, &view) {
            return Err(InvalidPolicy("access view binding or revision changed"));
        }
        let view = Arc::new(view);
        let previous = std::mem::replace(
            &mut *self
                .current
                .write()
                .map_err(|_| InvalidPolicy("access view publication failed"))?,
            view,
        );
        drop(previous);
        Ok(())
    }

    /// Publish a revision after the owner authorizes and commits its policy edit.
    /// Binding changes and older policy or volume revisions fail closed.
    pub fn publish(&self, view: PathTrie) -> Result<(), InvalidPolicy> {
        let view = Arc::new(view);
        let mut current = self
            .current
            .write()
            .map_err(|_| InvalidPolicy("access view publication failed"))?;
        if !can_replace(&current, &view) {
            return Err(InvalidPolicy("access view binding or revision changed"));
        }
        let previous = std::mem::replace(&mut *current, view);
        drop(current);
        drop(previous);
        Ok(())
    }
}

#[cfg(all(not(target_arch = "wasm32"), not(acyclic_fs_loom)))]
fn native_trie(
    mut view: PathTrie,
    mut custody: crate::NativeAllocation,
) -> Result<PathTrie, crate::NativeTreeMaterializationError> {
    if custody.bytes() != view.logical_owned_bytes() || view.native_custody.is_some() {
        return Err(crate::NativeTreeMaterializationError::InvalidLayout);
    }
    let mut header = custody.work().claim(
        (std::mem::size_of::<PathTrie>() + 2 * std::mem::size_of::<usize>()) as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    custody.absorb(&mut header)?;
    view.native_custody = Some(custody);
    Ok(view)
}

fn can_replace(current: &PathTrie, next: &PathTrie) -> bool {
    next.same_binding(current)
        && next.policy_version() >= current.policy_version()
        && next.head_position() >= current.head_position()
}
