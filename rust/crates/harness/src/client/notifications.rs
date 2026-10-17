//! Attention records projected from followed task streams, plus terminal encoding.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

/// Attention events owned by task/goal records, never inferred from renderer text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionEvent {
    /// Task has settled and is waiting for input.
    TaskSettled,
    /// Task failed.
    TaskFailed,
    /// Task is blocked.
    TaskBlocked,
    /// The file-driven goal reached done.
    GoalDone,
}

/// One notification in ClientViews; identity is its task and source stream offset.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
            AttentionEvent::GoalDone => "Goal done",
        }
    }
}

/// Bounded notification component of ClientViews. A full queue returns backpressure
/// before acknowledging a source offset, so a caller can resume from that record.
#[derive(Clone, Debug, Serialize)]
pub struct NotificationView {
    latest: BTreeMap<String, Arc<AttentionNotification>>,
    pending: VecDeque<Arc<AttentionNotification>>,
    maximum_tasks: usize,
    maximum_pending: usize,
    maximum_text_bytes: usize,
}

impl NotificationView {
    /// Chooses bounds independently of UI interest. Attention for offscreen tasks
    /// belongs here too; changing focus must not drop these records.
    pub fn new(
        maximum_tasks: usize,
        maximum_pending: usize,
        maximum_text_bytes: usize,
    ) -> Result<Self> {
        if maximum_tasks == 0 || maximum_pending == 0 || maximum_text_bytes == 0 {
            return Err(Error::Invalid(
                "notification bounds must be positive".into(),
            ));
        }
        Ok(Self {
            latest: BTreeMap::new(),
            pending: VecDeque::new(),
            maximum_tasks,
            maximum_pending,
            maximum_text_bytes,
        })
    }

    /// Projects a followed attention record. Older offsets are replay; an exact
    /// duplicate is idempotent, and conflicting reuse of the latest offset fails.
    pub fn apply(&mut self, notification: AttentionNotification) -> Result<bool> {
        if notification.sequence == 0
            || notification.task.trim().is_empty()
            || notification
                .task
                .len()
                .checked_add(notification.detail.len())
                .is_none_or(|bytes| bytes > self.maximum_text_bytes)
        {
            return Err(Error::Invalid("invalid attention record".into()));
        }
        if let Some(previous) = self.latest.get(&notification.task) {
            if notification.sequence < previous.sequence {
                return Ok(false);
            }
            if notification.sequence == previous.sequence {
                return if previous.as_ref() == &notification {
                    Ok(false)
                } else {
                    Err(Error::Conflict("attention offset was reused".into()))
                };
            }
        } else if self.latest.len() >= self.maximum_tasks {
            return Err(Error::Conflict("notification task bound reached".into()));
        }
        if self.pending.len() >= self.maximum_pending {
            return Err(Error::Conflict(
                "notification consumer must drain before advancing".into(),
            ));
        }
        let notification = Arc::new(notification);
        if let Some(previous) = self.latest.get_mut(&notification.task) {
            *previous = Arc::clone(&notification);
        } else {
            self.latest
                .insert(notification.task.clone(), Arc::clone(&notification));
        }
        self.pending.push_back(notification);
        Ok(true)
    }

    /// Borrows the next attention event; presentation failure leaves it pending.
    pub fn front(&self) -> Option<&AttentionNotification> {
        self.pending.front().map(Arc::as_ref)
    }
    /// Removes a presented or deliberately suppressed event. Its immutable
    /// payload stays shared with replay tracking and any other consumers.
    pub fn pop(&mut self) -> Option<Arc<AttentionNotification>> {
        self.pending.pop_front()
    }
    /// Number of pending presentation events.
    pub fn len(&self) -> usize {
        self.pending.len()
    }
    /// Whether presentation has caught up.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Turns an owner-authorized task-volume notification query into the client
    /// notification stream. Demand drives the source follow directly; there is
    /// no spawned pump, timer or offscreen-event conflation. Source errors stop
    /// delivery so the owner can reopen at its retained query cursor.
    pub fn follow<'a>(
        self,
        source: acyclic_stream::BoxProviderStream<'a, Result<AttentionNotification>>,
    ) -> impl futures::Stream<Item = Result<Arc<AttentionNotification>>> + 'a {
        use futures::StreamExt as _;
        futures::stream::try_unfold((self, source), |(mut view, mut source)| async move {
            loop {
                if let Some(notification) = view.pop() {
                    return Ok(Some((notification, (view, source))));
                }
                let Some(notification) = source.next().await else {
                    return Ok(None);
                };
                view.apply(notification?)?;
            }
        })
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

/// Presents pending notifications one at a time. GraphCoder writes these bytes
/// to its terminal, separately from JSONL/stdout wire records. A failed write
/// leaves the current item pending. The caller owns terminal IO scheduling.
pub fn write_terminal_notifications(
    view: &mut NotificationView,
    output: &mut impl std::io::Write,
    mode: NotificationMode,
    protocol: TerminalNotificationProtocol,
    focused: bool,
) -> std::io::Result<usize> {
    let mut presented = 0;
    while let Some(notification) = view.front() {
        let bytes = terminal_notification(notification, mode, protocol, focused);
        if !bytes.is_empty() {
            output.write_all(bytes.as_bytes())?;
            output.flush()?;
        }
        view.pop();
        presented += 1;
    }
    Ok(presented)
}
