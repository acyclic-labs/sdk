use super::*;

#[cfg(all(test, not(acyclic_harness_loom)))]
thread_local! {
    static IGNORE_IMAGE_POLICY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl CommandResolution {
    /// Moves a resolved prompt into the stock model command's input. Mentioned
    /// files retain their pinned revisions and use verified bounded text projection.
    /// Repeated file identities attach once, in first-mention order.
    pub fn into_model_input(self) -> Result<crate::model::ModelContent> {
        use crate::model::{FileProjectionPolicy, ModelContent, ModelContentPart};
        let Self::Prompt { text, mentions, .. } = self else {
            return Err(Error::Invalid(
                "command selects a view or typed operation".into(),
            ));
        };
        if mentions.is_empty() {
            let content = ModelContent::Text(text);
            content.validate_user_input(&mut None)?;
            return Ok(content);
        }
        // Borrow identities while selecting indices, then move refs into the input.
        // No file ref, path, version or prompt text is cloned for attachment.
        let mut seen = BTreeMap::new();
        let mut selected = Vec::with_capacity(mentions.len());
        for (index, mention) in mentions.iter().enumerate() {
            let file = &mention.file;
            let identity = (file.volume(), file.path(), file.version());
            if let Some(previous) = seen.insert(identity, mention) {
                if previous.file != mention.file || previous.image_input != mention.image_input {
                    return Err(Error::Conflict("mentioned file identity changed".into()));
                }
            } else {
                selected.push(index);
            }
        }
        drop(seen);
        let mut parts = Vec::with_capacity(selected.len() + 1);
        parts.push(ModelContentPart::Text { text });
        let mut selected = selected.into_iter().peekable();
        for (index, mention) in mentions.into_iter().enumerate() {
            if selected.peek() == Some(&index) {
                selected.next();
                parts.push(ModelContentPart::File {
                    policy: if mention.file.descriptor().media_type().starts_with("image/") {
                        let image = mention.image_input.ok_or_else(|| {
                            Error::Unsupported(
                                "image mention has no selected admission policy".into(),
                            )
                        })?;
                        image.validate()?;
                        let maximum_work = u64::from(image.maximum_tokens);
                        #[cfg(all(test, not(acyclic_harness_loom)))]
                        let maximum_work = if IGNORE_IMAGE_POLICY.with(std::cell::Cell::get) {
                            16_384
                        } else {
                            maximum_work
                        };
                        FileProjectionPolicy::Native(Box::new(crate::model::NativeMediaPolicy {
                            intent: crate::model::NativeMediaIntent::Image {
                                detail: crate::model::ImageDetail::Auto,
                            },
                            maximum_bytes: image.maximum_bytes,
                            maximum_work,
                            configuration: None,
                        }))
                    } else {
                        FileProjectionPolicy::BoundedFull
                    },
                    file: mention.file,
                });
            }
        }
        let content = ModelContent::Parts(parts);
        content.validate_user_input(&mut None)?;
        Ok(content)
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
#[path = "model_input_tests.rs"]
mod tests;
