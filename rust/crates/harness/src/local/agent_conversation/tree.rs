//! The one lazy preorder walk of an agent tree, shared by every whole-tree
//! projection.
use super::*;
use std::collections::VecDeque;

struct Frame {
    agent: AgentPath,
    children: VecDeque<AgentPath>,
    /// `None` once the last page has been read.
    next: Option<Option<String>>,
}

impl Frame {
    fn new(agent: AgentPath) -> Self {
        Self {
            agent,
            children: VecDeque::new(),
            next: Some(None),
        }
    }
}

/// Yields `root`, then each descendant in preorder. Memory is one children
/// page per level of depth, never the tree's size.
pub(super) struct TreeWalk {
    root: Option<AgentPath>,
    stack: Vec<Frame>,
}

impl TreeWalk {
    pub(super) fn new(root: &AgentPath) -> Self {
        Self {
            root: Some(root.clone()),
            stack: Vec::new(),
        }
    }

    pub(super) async fn next(&mut self, connection: &Connection) -> Result<Option<AgentPath>> {
        if let Some(root) = self.root.take() {
            self.stack.push(Frame::new(root.clone()));
            return Ok(Some(root));
        }
        while let Some(frame) = self.stack.last_mut() {
            if let Some(child) = frame.children.pop_front() {
                self.stack.push(Frame::new(child.clone()));
                return Ok(Some(child));
            }
            let Some(cursor) = frame.next.take() else {
                self.stack.pop();
                continue;
            };
            let page = AgentPageRequest::new(32, 32 * 1024, cursor).map_err(failed)?;
            let AgentReply::Children { items, next } = connection
                .execute(&AgentCommand::Children {
                    agent: frame.agent.clone(),
                    page,
                })
                .await?
            else {
                return Err(unexpected());
            };
            frame.children.extend(items.into_iter().map(|child| child.agent));
            frame.next = next.map(Some);
        }
        Ok(None)
    }
}
