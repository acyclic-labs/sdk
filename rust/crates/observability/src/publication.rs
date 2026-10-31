//! Fixed semantic evidence from the original executor publication.
use tracing::field::{Field, Visit};

/// The original journal publication's purpose, not a physical flush count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationPurpose {
    /// Accept the prepared model request before dispatch.
    ModelDispatch,
    /// Accept the original invocation before its effect.
    ToolIntent,
    /// Settle the original result and its projections.
    ToolSettlement,
    /// Record an inline refusal without claiming effect settlement.
    ToolFailureRecord,
}

/// Data named by the original publication producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationData {
    /// The prepared model request.
    PreparedRequest,
    /// The original recorded invocation.
    RecordedInvocation,
    /// The result and its associated projection.
    ResultAndProjection,
    /// An inline failure record.
    InlineFailure,
}

/// Control guarantee named by the original publication producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationControl {
    /// Append through the original conditional journal publication.
    ConditionalJournalAppend,
}

/// Optional, independent fields of `acyclic.harness.executor.publication`.
/// Missing or unrecognized values remain unknown. These semantic declarations
/// do not establish physical calls, publication range coverage or device drains.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Publication {
    /// Original publication purpose.
    pub purpose: Option<PublicationPurpose>,
    /// Original data semantics.
    pub data: Option<PublicationData>,
    /// Original control semantics.
    pub control: Option<PublicationControl>,
}

impl Visit for Publication {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "rpc" if self.purpose.is_none() => {
                self.purpose = match value {
                    "model-dispatch" => Some(PublicationPurpose::ModelDispatch),
                    "tool-intent" => Some(PublicationPurpose::ToolIntent),
                    "tool-settlement" => Some(PublicationPurpose::ToolSettlement),
                    "tool-failure-record" => Some(PublicationPurpose::ToolFailureRecord),
                    _ => None,
                };
            }
            "data" if self.data.is_none() => {
                self.data = match value {
                    "prepared-request" => Some(PublicationData::PreparedRequest),
                    "recorded-invocation" => Some(PublicationData::RecordedInvocation),
                    "result-and-projection" => Some(PublicationData::ResultAndProjection),
                    "inline-failure" => Some(PublicationData::InlineFailure),
                    _ => None,
                };
            }
            "control" if self.control.is_none() => {
                self.control = match value {
                    "conditional-journal-append" => {
                        Some(PublicationControl::ConditionalJournalAppend)
                    }
                    _ => None,
                };
            }
            _ => {}
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
