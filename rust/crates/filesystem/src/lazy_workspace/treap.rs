use super::*;

pub(super) async fn insert_overlay<S: LazyWorkspaceStore>(
    store: &S,
    mut root: LazyOverlayId,
    input: (LazyOverlay, KernelOwnedAllocation),
    ctx: &mut KernelAccounting<'_>,
) -> Result<LazyOverlayId, LazyWorkspaceError> {
    let mut input = Some(input);
    let mut parents = Owned::allocate(0, ctx)?;
    let result = async {
        loop {
            let current = store
                .load_lazy_overlay(root, ctx)
                .await?
                .ok_or(LazyWorkspaceError::NotFound)?;
            let LazyOverlay::Node {
                path: key,
                left,
                right,
                ..
            } = &current.0
            else {
                retire_node(current, ctx)?;
                let LazyOverlay::Node { left, right, .. } =
                    &mut input.as_mut().expect("pending input").0
                else {
                    unreachable!()
                };
                *left = root;
                *right = root;
                break;
            };
            let LazyOverlay::Node {
                path: input_key, ..
            } = &input.as_ref().expect("pending input").0
            else {
                unreachable!()
            };
            let direction = input_key.cmp(key);
            if direction.is_eq() {
                let children = (*left, *right);
                retire_node(current, ctx)?;
                let LazyOverlay::Node { left, right, .. } =
                    &mut input.as_mut().expect("pending input").0
                else {
                    unreachable!()
                };
                (*left, *right) = children;
                break;
            }
            root = if direction.is_lt() { *left } else { *right };
            if let Err(error) = parents.ensure_for_push(usize::MAX, ctx) {
                retire_node(current, ctx)?;
                return Err(error.into());
            }
            parents.value.push((current, direction));
        }
        root = store_overlay(store, input.take().expect("new leaf"), ctx).await?;
        while let Some((mut parent, direction)) = parents.value.pop() {
            let fetched = store.load_lazy_overlay(root, ctx).await;
            let mut child = match fetched {
                Ok(Some(child)) => child,
                result => {
                    retire_node(parent, ctx)?;
                    return Err(result.err().unwrap_or(LazyWorkspaceError::NotFound));
                }
            };
            let LazyOverlay::Node {
                priority,
                left,
                right,
                ..
            } = &mut parent.0
            else {
                unreachable!()
            };
            let LazyOverlay::Node {
                priority: child_priority,
                left: child_left,
                right: child_right,
                ..
            } = &mut child.0
            else {
                retire_node(parent, ctx)?;
                retire_node(child, ctx)?;
                return Err(LazyWorkspaceError::NotFound);
            };
            if child_priority > priority {
                if direction.is_lt() {
                    *left = *child_right;
                } else {
                    *right = *child_left;
                }
                let rotated = match store_overlay(store, parent, ctx).await {
                    Ok(id) => id,
                    Err(error) => {
                        retire_node(child, ctx)?;
                        return Err(error);
                    }
                };
                if direction.is_lt() {
                    *child_right = rotated;
                } else {
                    *child_left = rotated;
                }
                root = store_overlay(store, child, ctx).await?;
            } else {
                if direction.is_lt() {
                    *left = root;
                } else {
                    *right = root;
                }
                retire_node(child, ctx)?;
                root = store_overlay(store, parent, ctx).await?;
            }
        }
        Ok(root)
    }
    .await;
    if let Some(input) = input {
        retire_node(input, ctx)?;
    }
    for (node, _) in parents.value.drain(..) {
        retire_node(node, ctx)?;
    }
    parents.retire(ctx)?;
    result
}
async fn store_overlay<S: LazyWorkspaceStore>(
    store: &S,
    node: (LazyOverlay, KernelOwnedAllocation),
    ctx: &mut KernelAccounting<'_>,
) -> Result<LazyOverlayId, LazyWorkspaceError> {
    let id = match hash_lazy_value(&node.0, ctx) {
        Ok(digest) => LazyOverlayId(digest),
        Err(error) => {
            retire_node(node, ctx)?;
            return Err(error);
        }
    };
    store.put_lazy_overlay(id, node, ctx).await?;
    Ok(id)
}

pub(super) async fn insert_shadow<S: LazyWorkspaceStore>(
    store: &S,
    mut root: LazyShadowId,
    input: (LazyShadow, KernelOwnedAllocation),
    ctx: &mut KernelAccounting<'_>,
) -> Result<LazyShadowId, LazyWorkspaceError> {
    let mut input = Some(input);
    let mut parents = Owned::allocate(0, ctx)?;
    let result = async {
        loop {
            let current = store
                .load_lazy_shadow(root, ctx)
                .await?
                .ok_or(LazyWorkspaceError::NotFound)?;
            let LazyShadow::Node {
                file_id: key,
                left,
                right,
                ..
            } = &current.0
            else {
                retire_node(current, ctx)?;
                let LazyShadow::Node { left, right, .. } =
                    &mut input.as_mut().expect("pending input").0
                else {
                    unreachable!()
                };
                *left = root;
                *right = root;
                break;
            };
            let LazyShadow::Node {
                file_id: input_key, ..
            } = &input.as_ref().expect("pending input").0
            else {
                unreachable!()
            };
            let direction = input_key.cmp(key);
            if direction.is_eq() {
                let children = (*left, *right);
                retire_node(current, ctx)?;
                let LazyShadow::Node { left, right, .. } =
                    &mut input.as_mut().expect("pending input").0
                else {
                    unreachable!()
                };
                (*left, *right) = children;
                break;
            }
            root = if direction.is_lt() { *left } else { *right };
            if let Err(error) = parents.ensure_for_push(usize::MAX, ctx) {
                retire_node(current, ctx)?;
                return Err(error.into());
            }
            parents.value.push((current, direction));
        }
        root = store_shadow(store, input.take().expect("new leaf"), ctx).await?;
        while let Some((mut parent, direction)) = parents.value.pop() {
            let fetched = store.load_lazy_shadow(root, ctx).await;
            let mut child = match fetched {
                Ok(Some(child)) => child,
                result => {
                    retire_node(parent, ctx)?;
                    return Err(result.err().unwrap_or(LazyWorkspaceError::NotFound));
                }
            };
            let LazyShadow::Node {
                priority,
                left,
                right,
                ..
            } = &mut parent.0
            else {
                unreachable!()
            };
            let LazyShadow::Node {
                priority: child_priority,
                left: child_left,
                right: child_right,
                ..
            } = &mut child.0
            else {
                retire_node(parent, ctx)?;
                retire_node(child, ctx)?;
                return Err(LazyWorkspaceError::NotFound);
            };
            if child_priority > priority {
                if direction.is_lt() {
                    *left = *child_right;
                } else {
                    *right = *child_left;
                }
                let rotated = match store_shadow(store, parent, ctx).await {
                    Ok(id) => id,
                    Err(error) => {
                        retire_node(child, ctx)?;
                        return Err(error);
                    }
                };
                if direction.is_lt() {
                    *child_right = rotated;
                } else {
                    *child_left = rotated;
                }
                root = store_shadow(store, child, ctx).await?;
            } else {
                if direction.is_lt() {
                    *left = root;
                } else {
                    *right = root;
                }
                retire_node(child, ctx)?;
                root = store_shadow(store, parent, ctx).await?;
            }
        }
        Ok(root)
    }
    .await;
    if let Some(input) = input {
        retire_node(input, ctx)?;
    }
    for (node, _) in parents.value.drain(..) {
        retire_node(node, ctx)?;
    }
    parents.retire(ctx)?;
    result
}
async fn store_shadow<S: LazyWorkspaceStore>(
    store: &S,
    node: (LazyShadow, KernelOwnedAllocation),
    ctx: &mut KernelAccounting<'_>,
) -> Result<LazyShadowId, LazyWorkspaceError> {
    let id = match hash_lazy_value(&node.0, ctx) {
        Ok(digest) => LazyShadowId(digest),
        Err(error) => {
            retire_node(node, ctx)?;
            return Err(error);
        }
    };
    store.put_lazy_shadow(id, node, ctx).await?;
    Ok(id)
}
