//! A bounded projection of the canonical context file's scalar header. The
//! accounting/capacity/input fields precede all variable-length message data.
use super::*;
use crate::executor::json::{JsonBounds, JsonSpans, SpanEvent, SpanKind};

pub(super) async fn read(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
) -> Result<ContextUsage> {
    // A payload read page, not a limit on context, message or token counts.
    // The closed scalar header fits before count.message_tokens begins.
    let bytes = journal
        .load_range(
            reference,
            acyclic_fs::ByteRange {
                offset: 0,
                length: reference.descriptor().byte_length().min(256),
            },
            None,
        )?
        .await?;
    prefix(&bytes)
}

fn prefix(bytes: &[u8]) -> Result<ContextUsage> {
    let mut accounting = false;
    let mut section = false;
    let mut input = false;
    let mut capacity = None;
    let mut result = None;
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
            let slice = |start: u64, end: u64| &bytes[start as usize..end as usize];
            match event {
                SpanEvent::Key {
                    depth: 1,
                    start,
                    end,
                } => {
                    accounting = slice(start, end) == br#""accounting""#;
                }
                SpanEvent::Key {
                    depth: 2,
                    start,
                    end,
                } if accounting => {
                    section = slice(start, end) == br#""capacity""#;
                }
                SpanEvent::Key {
                    depth: 3,
                    start,
                    end,
                } if accounting => {
                    input = if section {
                        slice(start, end) == br#""context_tokens""#
                    } else {
                        slice(start, end) == br#""input_tokens""#
                    };
                }
                SpanEvent::End {
                    depth: 1,
                    kind: SpanKind::Scalar,
                    start,
                    end,
                } if accounting => {
                    if slice(start, end) == b"null" {
                        result = Some(ContextUsage::Unavailable);
                        return Ok(true);
                    }
                }
                SpanEvent::End {
                    depth: 3,
                    kind: SpanKind::Scalar,
                    start,
                    end,
                } if accounting && input => {
                    if section {
                        capacity = Some(
                            crate::contract::json_from_slice::<u32>(slice(start, end))
                                .map_err(|error| Error::Invalid(error.to_string()))?,
                        );
                    } else if let Some(capacity_tokens) = capacity.filter(|capacity| *capacity != 0)
                    {
                        result = Some(ContextUsage::Counted {
                            input_tokens: crate::contract::json_from_slice(slice(start, end))
                                .map_err(|error| Error::Invalid(error.to_string()))?,
                            capacity_tokens,
                        });
                        return Ok(true);
                    }
                }
                _ => {}
            }
            Ok(false)
        },
    )?;
    result.ok_or_else(|| Error::Storage("canonical context accounting header is malformed".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn accounting_header_is_independent_of_the_unread_context(
            input_tokens in any::<u64>(), capacity_tokens in 1..=u32::MAX, fixed in any::<u32>(), output in any::<u32>(),
        ) {
            // Stop before the variable-size planning weights and context. The
            // shared span cursor must need neither the remaining JSON nor EOF.
            let header = format!("{{\"accounting\":{{\"capacity\":{{\"context_tokens\":{capacity_tokens},\"output_tokens\":{output}}},\"count\":{{\"fixed_tokens\":{fixed},\"input_tokens\":{input_tokens},\"message_tokens\":[");
            prop_assert_eq!(prefix(&header.as_bytes()[..header.len().min(256)]).unwrap(), ContextUsage::Counted { input_tokens, capacity_tokens });
        }
    }
}
