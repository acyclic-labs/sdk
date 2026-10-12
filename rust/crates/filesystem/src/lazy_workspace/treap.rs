use super::*;
use crate::kernel::{KernelAccounting, Owned};

pub(super) struct ImmutableTreapNode<K, V, I> {
    pub(super) key: K,
    pub(super) priority: u64,
    pub(super) value: V,
    pub(super) left: I,
    pub(super) right: I,
}

pub(super) trait ImmutableTreap: Sync {
    type Id: Copy + Send + Sync;
    type Key: Ord + Send + Sync;
    type Value: Send + Sync;
    fn load(
        &self,
        id: Self::Id,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<
        Output = Result<
            Option<Owned<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>>,
            LazyWorkspaceError,
        >,
    > + Send;
    fn store(
        &self,
        node: Owned<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<Self::Id, LazyWorkspaceError>> + Send;
    fn priority(key: &Self::Key) -> u64;
    fn key_bytes(key: &Self::Key) -> u64;
}

pub(super) async fn insert_immutable_treap<T: ImmutableTreap>(
    treap: &T,
    mut root: T::Id,
    input: Owned<(T::Key, T::Value)>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<T::Id, LazyWorkspaceError> {
    let mut input = Some(input);
    let mut parents = Owned::allocate(0, accounting)?;
    let result = async {
        loop {
            let Some(node) = treap.load(root, accounting).await? else {
                break;
            };
            let direction = input
                .as_ref()
                .expect("pending key")
                .value
                .0
                .cmp(&node.value.key);
            if direction.is_eq() {
                let input = input.take().expect("pending replacement");
                let replacement = Owned::new(
                    ImmutableTreapNode {
                        key: input.value.0,
                        value: input.value.1,
                        priority: node.value.priority,
                        left: node.value.left,
                        right: node.value.right,
                    },
                    input.allocation,
                    input.bytes,
                );
                node.retire(accounting)?;
                root = treap.store(replacement, accounting).await?;
                break;
            }
            root = if direction.is_lt() {
                node.value.left
            } else {
                node.value.right
            };
            if let Err(error) = parents.ensure_for_push(usize::MAX, accounting) {
                node.retire(accounting)?;
                return Err(error.into());
            }
            parents.value.push((node, direction));
        }
        if let Some(input) = input.take() {
            accounting.charge(WorkCounters {
                bytes_hashed: T::key_bytes(&input.value.0),
                ..WorkCounters::UNCHARGED
            })?;
            let priority = T::priority(&input.value.0);
            let leaf = Owned::new(
                ImmutableTreapNode {
                    key: input.value.0,
                    value: input.value.1,
                    priority,
                    left: root,
                    right: root,
                },
                input.allocation,
                input.bytes,
            );
            root = treap.store(leaf, accounting).await?;
        }
        while let Some((mut parent, direction)) = parents.value.pop() {
            let child = treap.load(root, accounting).await?;
            if let Some(mut child) = child {
                if child.value.priority < parent.value.priority {
                    if direction.is_lt() {
                        parent.value.left = child.value.right;
                    } else {
                        parent.value.right = child.value.left;
                    }
                    let rotated = treap.store(parent, accounting).await?;
                    if direction.is_lt() {
                        child.value.right = rotated;
                    } else {
                        child.value.left = rotated;
                    }
                    root = treap.store(child, accounting).await?;
                    continue;
                }
                child.retire(accounting)?;
            }
            if direction.is_lt() {
                parent.value.left = root;
            } else {
                parent.value.right = root;
            }
            root = treap.store(parent, accounting).await?;
        }
        Ok(root)
    }
    .await;
    if let Some(input) = input {
        input.retire(accounting)?;
    }
    while let Some((node, _)) = parents.value.pop() {
        node.retire(accounting)?;
    }
    parents.retire(accounting)?;
    result
}

pub(super) struct OverlayTreap<'a, S>(pub(super) &'a S);

impl<S: LazyWorkspaceStore> ImmutableTreap for OverlayTreap<'_, S> {
    type Id = LazyOverlayId;
    type Key = String;
    type Value = LazyOverlayChange;

    async fn load(
        &self,
        id: Self::Id,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<
        Option<Owned<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>>,
        LazyWorkspaceError,
    > {
        let node = self
            .0
            .load_lazy_overlay(id, accounting)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        let bytes = node.0.owned_bytes();
        match node.0 {
            LazyOverlay::Empty => {
                crate::kernel::retire_allocation(node.1, bytes, accounting)?;
                Ok(None)
            }
            LazyOverlay::Node {
                path,
                priority,
                change,
                left,
                right,
            } => Ok(Some(Owned::new(
                ImmutableTreapNode {
                    key: path,
                    priority,
                    value: change,
                    left,
                    right,
                },
                node.1,
                bytes,
            ))),
        }
    }

    async fn store(
        &self,
        node: Owned<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self::Id, LazyWorkspaceError> {
        let value = LazyOverlay::Node {
            path: node.value.key,
            priority: node.value.priority,
            change: node.value.value,
            left: node.value.left,
            right: node.value.right,
        };
        let id = LazyOverlayId::from_bytes(hash_lazy_value(&value, accounting)?);
        self.0
            .put_lazy_overlay(id, (value, node.allocation), accounting)
            .await?;
        Ok(id)
    }

    fn priority(key: &Self::Key) -> u64 {
        path_priority(key)
    }
    fn key_bytes(key: &Self::Key) -> u64 {
        key.len() as u64
    }
}

pub(super) struct ShadowValue {
    pub(super) record: Vec<u8>,
    pub(super) metadata: Box<crate::WorkspaceMetadata>,
}

pub(super) struct ShadowTreap<'a, S>(pub(super) &'a S);

impl<S: LazyWorkspaceStore> ImmutableTreap for ShadowTreap<'_, S> {
    type Id = LazyShadowId;
    type Key = FileId;
    type Value = ShadowValue;

    async fn load(
        &self,
        id: Self::Id,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<
        Option<Owned<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>>,
        LazyWorkspaceError,
    > {
        let node = self
            .0
            .load_lazy_shadow(id, accounting)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        let bytes = node.0.owned_bytes();
        match node.0 {
            LazyShadow::Empty => {
                crate::kernel::retire_allocation(node.1, bytes, accounting)?;
                Ok(None)
            }
            LazyShadow::Node {
                file_id,
                priority,
                record,
                metadata,
                left,
                right,
            } => Ok(Some(Owned::new(
                ImmutableTreapNode {
                    key: file_id,
                    priority,
                    value: ShadowValue { record, metadata },
                    left,
                    right,
                },
                node.1,
                bytes,
            ))),
        }
    }

    async fn store(
        &self,
        node: Owned<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self::Id, LazyWorkspaceError> {
        let value = LazyShadow::Node {
            file_id: node.value.key,
            priority: node.value.priority,
            record: node.value.value.record,
            metadata: node.value.value.metadata,
            left: node.value.left,
            right: node.value.right,
        };
        let id = LazyShadowId::from_bytes(hash_lazy_value(&value, accounting)?);
        self.0
            .put_lazy_shadow(id, (value, node.allocation), accounting)
            .await?;
        Ok(id)
    }

    fn priority(key: &Self::Key) -> u64 {
        shadow_priority(*key)
    }
    fn key_bytes(_: &Self::Key) -> u64 {
        16
    }
}
