use super::*;


#[derive(Clone)]
pub(super) struct ImmutableTreapNode<K, V, I> {
    pub(super) key: K,
    pub(super) priority: u64,
    pub(super) value: V,
    pub(super) left: I,
    pub(super) right: I,
}

#[async_trait]
pub(super) trait ImmutableTreap: Sync {
    type Id: Copy + Send + Sync;
    type Key: Clone + Ord + Send + Sync;
    type Value: Clone + Send + Sync;

    async fn load(
        &self,
        id: Self::Id,
    ) -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>;

    async fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> Result<Self::Id, LazyWorkspaceError>;

    fn priority(key: &Self::Key) -> u64;
}

pub(super) fn insert_immutable_treap<'a, T: ImmutableTreap>(
    treap: &'a T,
    root: T::Id,
    key: T::Key,
    value: T::Value,
) -> LazyFuture<'a, Result<T::Id, LazyWorkspaceError>> {
    Box::pin(async move {
        let Some(current) = treap.load(root).await? else {
            return treap
                .store(ImmutableTreapNode {
                    priority: T::priority(&key),
                    key,
                    value,
                    left: root,
                    right: root,
                })
                .await;
        };
        match key.cmp(&current.key) {
            std::cmp::Ordering::Equal => {
                treap
                    .store(ImmutableTreapNode {
                        key,
                        value,
                        ..current
                    })
                    .await
            }
            ordering => {
                let descend_left = ordering == std::cmp::Ordering::Less;
                let child_root = if descend_left {
                    current.left
                } else {
                    current.right
                };
                let inserted = insert_immutable_treap(treap, child_root, key, value).await?;
                let child = treap.load(inserted).await?.ok_or_else(|| {
                    LazyWorkspaceError::Store("inserted lazy index node is absent".to_owned())
                })?;
                if child.priority > current.priority {
                    let rotated = treap
                        .store(if descend_left {
                            ImmutableTreapNode {
                                left: child.right,
                                ..current
                            }
                        } else {
                            ImmutableTreapNode {
                                right: child.left,
                                ..current
                            }
                        })
                        .await?;
                    return treap
                        .store(if descend_left {
                            ImmutableTreapNode {
                                right: rotated,
                                ..child
                            }
                        } else {
                            ImmutableTreapNode {
                                left: rotated,
                                ..child
                            }
                        })
                        .await;
                }
                treap
                    .store(if descend_left {
                        ImmutableTreapNode {
                            left: inserted,
                            ..current
                        }
                    } else {
                        ImmutableTreapNode {
                            right: inserted,
                            ..current
                        }
                    })
                    .await
            }
        }
    })
}

pub(super) async fn immutable_treap_value<T: ImmutableTreap>(
    treap: &T,
    mut root: T::Id,
    key: &T::Key,
) -> Result<Option<T::Value>, LazyWorkspaceError> {
    loop {
        let Some(node) = treap.load(root).await? else {
            return Ok(None);
        };
        match key.cmp(&node.key) {
            std::cmp::Ordering::Equal => return Ok(Some(node.value)),
            std::cmp::Ordering::Less => root = node.left,
            std::cmp::Ordering::Greater => root = node.right,
        }
    }
}

pub(super) struct OverlayTreap<'a, S>(pub(super) &'a S);

#[async_trait]
impl<S: LazyWorkspaceStore> ImmutableTreap for OverlayTreap<'_, S> {
    type Id = LazyOverlayId;
    type Key = String;
    type Value = LazyOverlayChange;

    async fn load(
        &self,
        id: Self::Id,
    ) -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>
    {
        match self.0.load_lazy_overlay(id).await.map_err(store_error)? {
            Some(LazyOverlay::Empty) => Ok(None),
            Some(LazyOverlay::Node {
                path,
                priority,
                change,
                left,
                right,
            }) => Ok(Some(ImmutableTreapNode {
                key: path,
                priority,
                value: change,
                left,
                right,
            })),
            None => Err(LazyWorkspaceError::Store(
                "lazy overlay is absent".to_owned(),
            )),
        }
    }

    async fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> Result<Self::Id, LazyWorkspaceError> {
        let overlay = LazyOverlay::Node {
            path: node.key,
            priority: node.priority,
            change: node.value,
            left: node.left,
            right: node.right,
        };
        let id = overlay.id()?;
        self.0
            .put_lazy_overlay(id, overlay)
            .await
            .map_err(store_error)?;
        Ok(id)
    }

    fn priority(key: &Self::Key) -> u64 {
        path_priority(key)
    }
}

#[derive(Clone)]
pub(super) struct ShadowValue {
    pub(super) record: Vec<u8>,
    pub(super) metadata: Box<crate::WorkspaceMetadata>,
}

pub(super) struct ShadowTreap<'a, S>(pub(super) &'a S);

#[async_trait]
impl<S: LazyWorkspaceStore> ImmutableTreap for ShadowTreap<'_, S> {
    type Id = LazyShadowId;
    type Key = FileId;
    type Value = ShadowValue;

    async fn load(
        &self,
        id: Self::Id,
    ) -> Result<Option<ImmutableTreapNode<Self::Key, Self::Value, Self::Id>>, LazyWorkspaceError>
    {
        match self.0.load_lazy_shadow(id).await.map_err(store_error)? {
            Some(LazyShadow::Empty) => Ok(None),
            Some(LazyShadow::Node {
                file_id,
                priority,
                record,
                metadata,
                left,
                right,
            }) => Ok(Some(ImmutableTreapNode {
                key: file_id,
                priority,
                value: ShadowValue { record, metadata },
                left,
                right,
            })),
            None => Err(LazyWorkspaceError::Store(
                "lazy identity shadow is absent".to_owned(),
            )),
        }
    }

    async fn store(
        &self,
        node: ImmutableTreapNode<Self::Key, Self::Value, Self::Id>,
    ) -> Result<Self::Id, LazyWorkspaceError> {
        let shadow = LazyShadow::Node {
            file_id: node.key,
            priority: node.priority,
            record: node.value.record,
            metadata: node.value.metadata,
            left: node.left,
            right: node.right,
        };
        let id = shadow.id()?;
        self.0
            .put_lazy_shadow(id, shadow)
            .await
            .map_err(store_error)?;
        Ok(id)
    }

    fn priority(key: &Self::Key) -> u64 {
        shadow_priority(*key)
    }
}

