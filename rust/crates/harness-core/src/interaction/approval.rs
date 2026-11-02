use super::*;

/// Exact effect intent that an approval authorizes, independent of display text.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ApprovalBinding {
    /// Operation to which the decision applies.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Digest of the exact approved arguments and action.
    pub action_digest: [u8; 32],
}

impl ApprovalBinding {
    /// Rejects the all-zero sentinel so every approval names an exact intent.
    pub fn validate(&self) -> Result<()> {
        if self.action_digest == [0; 32] {
            return Err(Error::Invalid(
                "approval action digest cannot be zero".into(),
            ));
        }
        Ok(())
    }
}

impl Interaction {
    /// Projects the complete explanation, if present in this bounded prefix of
    /// an original canonical approval request. A missing explanation means the
    /// caller needs more of the prefix, not that the request has no explanation.
    ///
    /// The request producer already validates the document. This uses the shared
    /// JSON span cursor and stops before the subject; it does not decode, copy,
    /// or validate the remainder of the request.
    pub fn approval_prompt_preview(bytes: &[u8]) -> Result<Option<String>> {
        use crate::executor::json::{JsonBounds, JsonSpans, SpanEvent, SpanKind};
        let mut prompt = false;
        let mut selected = None;
        JsonSpans::default().feed(
            bytes,
            JsonBounds {
                maximum_bytes: bytes.len() as u64,
                maximum_items: bytes.len() as u64,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            None,
            &mut |_| Ok(()),
            &mut |event| {
                match event {
                    SpanEvent::Key {
                        depth: 1,
                        start,
                        end,
                    } => {
                        prompt = &bytes[start as usize..end as usize] == br#""prompt""#;
                    }
                    SpanEvent::End {
                        depth: 1,
                        kind: SpanKind::String,
                        start,
                        end,
                    } if prompt => {
                        selected = Some((start as usize, end as usize));
                        return Ok(true);
                    }
                    _ => {}
                }
                Ok(false)
            },
        )?;
        selected
            .map(|(start, end)| crate::contract::json_from_slice(&bytes[start..end]))
            .transpose()
    }
}

crate::core::ownership::owned_struct!(ApprovalBinding {
    operation_id,
    action_digest
});
