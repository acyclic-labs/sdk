use super::*;

macro_rules! insert {
    ($insert:ident, $store:ident, $node:ident, $id:ident, $key:ident, $load:ident, $put:ident) => {
        pub(super) async fn $insert<S: LazyWorkspaceStore>(
            store: &S,
            mut root: $id,
            input: ($node, KernelOwnedAllocation),
            ctx: &mut KernelAccounting<'_>,
        ) -> Result<$id, LazyWorkspaceError> {
            let mut input = Some(input);
            let mut parents = Owned::allocate(0, ctx)?;
            let result = async {
                loop {
                    let current = store
                        .$load(root, ctx)
                        .await?
                        .ok_or(LazyWorkspaceError::NotFound)?;
                    let $node::Node {
                        $key: input_key,
                        left: input_left,
                        right: input_right,
                        ..
                    } = &mut input.as_mut().expect("pending input").0
                    else {
                        unreachable!()
                    };
                    let direction = match &current.0 {
                        $node::Empty => {
                            *input_left = root;
                            *input_right = root;
                            None
                        }
                        $node::Node {
                            $key: key,
                            left,
                            right,
                            ..
                        } => {
                            let direction = (&*input_key).cmp(key);
                            if direction.is_eq() {
                                *input_left = *left;
                                *input_right = *right;
                                None
                            } else {
                                root = if direction.is_lt() { *left } else { *right };
                                Some(direction)
                            }
                        }
                    };
                    let Some(direction) = direction else {
                        retire_node(current, ctx)?;
                        break;
                    };
                    if let Err(error) = parents.ensure_for_push(usize::MAX, ctx) {
                        retire_node(current, ctx)?;
                        return Err(error.into());
                    }
                    parents.value.push((current, direction));
                }
                root = $store(store, input.take().expect("new leaf"), ctx).await?;
                while let Some((mut parent, direction)) = parents.value.pop() {
                    let mut child = match store.$load(root, ctx).await {
                        Ok(Some(child)) => child,
                        result => {
                            retire_node(parent, ctx)?;
                            return Err(result.err().unwrap_or(LazyWorkspaceError::NotFound));
                        }
                    };
                    let $node::Node {
                        priority,
                        left,
                        right,
                        ..
                    } = &mut parent.0
                    else {
                        unreachable!()
                    };
                    let $node::Node {
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
                        let rotated = match $store(store, parent, ctx).await {
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
                        root = $store(store, child, ctx).await?;
                    } else {
                        if direction.is_lt() {
                            *left = root;
                        } else {
                            *right = root;
                        }
                        retire_node(child, ctx)?;
                        root = $store(store, parent, ctx).await?;
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
        async fn $store<S: LazyWorkspaceStore>(
            store: &S,
            node: ($node, KernelOwnedAllocation),
            ctx: &mut KernelAccounting<'_>,
        ) -> Result<$id, LazyWorkspaceError> {
            let id = match hash_lazy_value(&node.0, ctx) {
                Ok(digest) => $id(digest),
                Err(error) => {
                    retire_node(node, ctx)?;
                    return Err(error);
                }
            };
            store.$put(id, node, ctx).await?;
            Ok(id)
        }
    };
}
insert!(
    insert_overlay,
    store_overlay,
    LazyOverlay,
    LazyOverlayId,
    path,
    load_lazy_overlay,
    put_lazy_overlay
);
insert!(
    insert_shadow,
    store_shadow,
    LazyShadow,
    LazyShadowId,
    file_id,
    load_lazy_shadow,
    put_lazy_shadow
);
