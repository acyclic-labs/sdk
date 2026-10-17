//! Attention records projected from followed files, plus stateless terminal encoding.

use super::{NotificationFileView, NotificationQueryView};
use serde::{Deserialize, Serialize};

/// Attention events owned by task/goal records, never inferred from renderer text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum AttentionEvent {
    /// Task has settled and is waiting for input.
    TaskSettled,
    /// Task failed.
    TaskFailed,
    /// Task is blocked.
    TaskBlocked,
    /// A harness approval request is waiting for its configured approver.
    NeedsApproval,
    /// The file-driven goal cannot make progress without input.
    GoalBlocked,
    /// The file-driven goal reached done.
    GoalDone,
}

/// One notification file in ClientViews; identity is its task and source sequence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct AttentionNotification {
    /// Stable task identity supplied by the followed stream.
    pub task: String,
    /// Positive committed source offset, retained through replay and reconnect.
    pub sequence: u64,
    /// Machine-owned attention event.
    pub event: AttentionEvent,
    /// Optional human-readable detail. Terminal adapters treat it as untrusted text.
    pub detail: String,
}

impl AttentionNotification {
    /// Maps committed lifecycle events without interpreting display text.
    /// Waiting for children or effects does not itself mean a task is blocked.
    pub fn lifecycle(
        task: String,
        sequence: u64,
        state: crate::core::LifecycleState,
        detail: String,
    ) -> Option<Self> {
        let event = match state {
            crate::core::LifecycleState::Completed => AttentionEvent::TaskSettled,
            crate::core::LifecycleState::Failed => AttentionEvent::TaskFailed,
            _ => return None,
        };
        Some(Self {
            task,
            sequence,
            event,
            detail,
        })
    }
    /// Canonical presentation title shared by TUI and browser adapters.
    pub fn title(&self) -> &'static str {
        match self.event {
            AttentionEvent::TaskSettled => "Task settled",
            AttentionEvent::TaskFailed => "Task failed",
            AttentionEvent::TaskBlocked => "Task blocked",
            AttentionEvent::NeedsApproval => "Approval needed",
            AttentionEvent::GoalBlocked => "Goal blocked",
            AttentionEvent::GoalDone => "Goal done",
        }
    }
}

/// Terminal notification preference. These settings never discard ClientViews data.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NotificationMode {
    /// All specified attention events.
    #[default]
    On,
    /// No terminal side effects.
    Off,
    /// Notify for failures, blocked tasks and completed goals; ordinary settles
    /// remain visible in ClientViews without interrupting the desktop.
    AttentionOnly,
}

/// Selected terminal capability; no terminal probing or polling is performed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalNotificationProtocol {
    /// OSC 9, supported by terminals including iTerm2.
    Osc9,
    /// OSC 777 notify, including a separate title and body.
    Osc777,
    /// Audible bell when no notification escape is supported.
    Bell,
}

/// Encodes GraphCoder's terminal side effect. The TUI supplies focus from input
/// events. Focused users already see the view, so desktop alerts are suppressed.
pub fn terminal_notification(
    notification: &AttentionNotification,
    mode: NotificationMode,
    protocol: TerminalNotificationProtocol,
    focused: bool,
) -> String {
    if focused
        || mode == NotificationMode::Off
        || mode == NotificationMode::AttentionOnly
            && notification.event == AttentionEvent::TaskSettled
    {
        return String::new();
    }
    let (prefix, separator) = match protocol {
        TerminalNotificationProtocol::Bell => return "\x07".into(),
        TerminalNotificationProtocol::Osc9 => ("\x1b]9;", ": "),
        TerminalNotificationProtocol::Osc777 => ("\x1b]777;notify;", ";"),
    };
    let title = notification.title();
    let mut output = String::with_capacity(
        prefix.len()
            + title.len()
            + separator.len()
            + notification.task.len()
            + 2
            + notification.detail.len()
            + 1,
    );
    output.push_str(prefix);
    output.push_str(title);
    output.push_str(separator);
    append_terminal_text(&mut output, &notification.task);
    output.push_str(": ");
    append_terminal_text(&mut output, &notification.detail);
    output.push('\x07');
    output
}

fn append_terminal_text(output: &mut String, text: &str) {
    for ch in text.chars() {
        output.push(if ch.is_control() || ch == ';' {
            ' '
        } else {
            ch
        });
    }
}

/// Presents current rows directly from the shared file projection. No terminal
/// queue retains removed or replaced files. The caller retries from its current
/// query after an IO failure and owns any persisted presentation cursor.
pub fn write_terminal_notifications<'a>(
    rows: impl IntoIterator<Item = NotificationQueryView<'a>>,
    output: &mut impl std::io::Write,
    mode: NotificationMode,
    protocol: TerminalNotificationProtocol,
    focused: bool,
) -> std::io::Result<usize> {
    let mut presented = 0;
    for row in rows {
        let NotificationFileView::Present { notification } = row.notification else {
            continue;
        };
        let bytes = terminal_notification(notification, mode, protocol, focused);
        if !bytes.is_empty() {
            output.write_all(bytes.as_bytes())?;
            output.flush()?;
        }
        presented += 1;
    }
    Ok(presented)
}
