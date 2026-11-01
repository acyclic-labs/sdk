//! The Git filesystem actions the workspace executor performs.
use super::*;

impl<F, Fut, P, O, S> WorkspaceGitExecutor<'_, F, P, O, S>
