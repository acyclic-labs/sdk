//! Temporary task-file projection, retained for #626 until the #778 follow-up.
use super::*;
mod usage;
pub(in crate::client) mod views;
pub use views::{ClientViews, TaskView, ToolCallView, TranscriptView, ViewUpdate};
mod file_query;
pub use file_query::{FileQueryView, GoalFileView};
mod goal_view;
pub use goal_view::{GoalProgress, GoalQueryView};
mod goal_selection;
mod model_view;
pub use model_view::{ModelFileView, ModelQueryView};
#[cfg(feature = "tui")]
mod goal_panel;
#[cfg(feature = "tui")]
pub use goal_panel::GoalPanel;
#[cfg(feature = "tui")]
mod plan_panel;
#[cfg(feature = "tui")]
pub use plan_panel::PlanPanel;
