//! Pure view/editor preferences; defaults are compiled from data/settings.json.
/// Notification filtering selected by the settings file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "tui", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "tui", serde(rename_all = "kebab-case"))]
pub enum NotificationPolicy {
    /// All attention events.
    On,
    /// No terminal notification.
    Off,
    /// Only events requiring attention.
    AttentionOnly,
}

/// Editor and view preferences projected from configuration files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "tui", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "tui", serde(deny_unknown_fields))]
pub struct UiSettings {
    /// scrollback lines.
    pub scrollback_lines: usize,
    /// Render activity labels without animation.
    pub reduced_motion: bool,
    /// wrap.
    pub wrap: bool,
    /// notifications.
    pub notifications: NotificationPolicy,
    /// notify when focused.
    pub notify_when_focused: bool,
    /// max input height percent.
    pub max_input_height_percent: usize,
    /// tab width.
    pub tab_width: usize,
    /// paste collapse bytes.
    pub paste_collapse_bytes: usize,
    /// paste collapse lines.
    pub paste_collapse_lines: usize,
    /// undo group ms.
    pub undo_group_ms: usize,
    /// undo group graphemes.
    pub undo_group_graphemes: usize,
    /// chord timeout ms.
    pub chord_timeout_ms: usize,
    /// double interrupt ms.
    pub double_interrupt_ms: usize,
    /// multi click ms.
    pub multi_click_ms: usize,
    /// cursor idle ms.
    pub cursor_idle_ms: usize,
    /// cursor cycle ms.
    pub cursor_cycle_ms: usize,
    /// activity ms.
    pub activity_ms: usize,
    /// completion max width.
    pub completion_max_width: usize,
    /// completion max height.
    pub completion_max_height: usize,
    /// image max dimension.
    pub image_max_dimension: usize,
    /// image quality.
    pub image_quality: usize,
}
include!(concat!(env!("OUT_DIR"), "/settings.rs"));
impl Default for UiSettings {
    fn default() -> Self {
        DEFAULT_SETTINGS
    }
}

impl UiSettings {
    /// Visible transcript tail. The durable transcript remains in its volume.
    pub fn scrollback<'a, T>(&self, lines: &'a [T]) -> &'a [T] {
        lines
            .get(lines.len().saturating_sub(self.scrollback_lines)..)
            .unwrap_or_default()
    }
}
