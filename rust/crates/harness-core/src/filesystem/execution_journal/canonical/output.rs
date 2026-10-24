use super::*;

/// Select the authoritative text while reading each saved event only once.
#[derive(Default)]
pub(super) struct CanonicalSummaryOutput {
    text: String,
    completed_message: bool,
    fallback_overflow: bool,
}

impl CanonicalSummaryOutput {
    pub(super) fn observe(
        &mut self,
        event: &ModelEvent,
        maximum: u64,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        match event {
            ModelEvent::Content { delta } if !self.completed_message && !self.fallback_overflow => {
                // A later completed message can supersede an oversized streamed
                // prefix. Defer rejection without retaining the excess bytes.
                if self
                    .text
                    .len()
                    .checked_add(delta.len())
                    .is_none_or(|length| length as u64 > maximum)
                {
                    self.fallback_overflow = true;
                } else {
                    canonical_push_text(&mut self.text, delta, maximum, io)?;
                }
            }
            ModelEvent::MessageCompleted { text } => {
                self.text.clear();
                canonical_push_text(&mut self.text, text, maximum, io)?;
                self.completed_message = true;
                self.fallback_overflow = false;
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn finish(self, io: Option<&PreparationIo>) -> Result<String> {
        if self.fallback_overflow {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "canonical summary output exceeds bounds",
            ));
        }
        Ok(self.text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saved_summary_fallback_is_bounded_until_an_authoritative_message_arrives() -> Result<()> {
        for final_text in [None, Some(""), Some("λ")] {
            let mut output = CanonicalSummaryOutput::default();
            for delta in ["ab", "cdef", "ignored"] {
                output.observe(
                    &ModelEvent::Content {
                        delta: delta.into(),
                    },
                    4,
                    None,
                )?;
                assert!(output.text.len() <= 4);
            }
            if let Some(text) = final_text {
                output.observe(&ModelEvent::MessageCompleted { text: text.into() }, 4, None)?;
                output.observe(
                    &ModelEvent::Content {
                        delta: "later delta".into(),
                    },
                    4,
                    None,
                )?;
                assert_eq!(output.finish(None)?, text);
            } else {
                assert!(matches!(output.finish(None), Err(Error::Invalid(_))));
            }
        }
        Ok(())
    }

    #[test]
    fn saved_summary_preserves_legacy_deltas_and_the_last_completed_message() -> Result<()> {
        let mut output = CanonicalSummaryOutput::default();
        for delta in ["a", "λ"] {
            output.observe(
                &ModelEvent::Content {
                    delta: delta.into(),
                },
                4,
                None,
            )?;
        }
        assert_eq!(output.finish(None)?, "aλ");
        let mut output = CanonicalSummaryOutput::default();
        for text in ["old", "λ", ""] {
            output.observe(&ModelEvent::MessageCompleted { text: text.into() }, 4, None)?;
        }
        assert_eq!(output.finish(None)?, "");
        let mut output = CanonicalSummaryOutput::default();
        assert!(matches!(
            output.observe(
                &ModelEvent::MessageCompleted {
                    text: "large".into()
                },
                4,
                None
            ),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }
}
