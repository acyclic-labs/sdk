use super::wire::Item;
use crate::{Adapter, ResolvedModel, invalid};
use acyclic_harness::{
    Result,
    model::{
        FileProjectionPolicy, ModelContent, ModelContentPart, ModelDataPart, ToolResultContent,
    },
};
use serde_json::json;

pub(super) struct Media<'p, 'i, 'b> {
    pub provider: &'p ResolvedModel,
    pub files: &'i mut dyn Iterator<Item = &'b [u8]>,
    pub text_files: &'i mut crate::files::FileText,
    pub planning: bool,
}
impl<'p, 'i, 'a> Media<'p, 'i, 'a> {
    pub fn file(
        &mut self,
        file: &'a acyclic_harness::conversation::FileRef,
        policy: &'a FileProjectionPolicy,
        allowed: bool,
    ) -> Result<Item<'a>> {
        file_part(
            self.provider,
            file,
            policy,
            self.files,
            self.planning,
            allowed,
            self.text_files,
        )
    }
    pub fn text(&mut self, content: &'a ModelContent) -> Result<String> {
        text_content(
            self.provider,
            content,
            self.files,
            self.planning,
            self.text_files,
        )
    }
    pub fn result(&mut self, content: &'a ToolResultContent) -> Result<Vec<Item<'a>>> {
        result_parts(
            self.provider,
            content,
            self.files,
            self.planning,
            self.text_files,
        )
    }
}

fn text_part(adapter: Adapter, text: &str) -> Item<'static> {
    Item::Json(match adapter {
        Adapter::Gemini => json!({"text":text}),
        Adapter::Responses => json!({"type":"input_text","text":text}),
        _ => json!({"type":"text","text":text}),
    })
}

pub(super) fn result_parts<'a>(
    provider: &ResolvedModel,
    content: &'a ToolResultContent,
    images: &mut dyn Iterator<Item = &'a [u8]>,
    counting: bool,
    text_files: &mut crate::files::FileText,
) -> Result<Vec<Item<'a>>> {
    match content {
        ToolResultContent::Json { value } => {
            let text = serde_json::to_string(value).map_err(|error| invalid(error.to_string()))?;
            Ok(vec![text_part(provider.config.adapter, &text)])
        }
        ToolResultContent::Parts { parts } => parts
            .iter()
            .map(|part| match part {
                ModelDataPart::Text { text } => Ok(text_part(provider.config.adapter, text)),
                ModelDataPart::File { file, policy } => {
                    file_part(provider, file, policy, images, counting, true, text_files)
                }
            })
            .collect(),
    }
}

pub(super) fn text_content<'a>(
    provider: &ResolvedModel,
    content: &'a ModelContent,
    images: &mut dyn Iterator<Item = &'a [u8]>,
    counting: bool,
    text_files: &mut crate::files::FileText,
) -> Result<String> {
    if let ModelContent::Text(text) = content {
        return Ok(text.clone());
    }
    content
        .parts()
        .iter()
        .map(|part| match part {
            ModelContentPart::Text { text } => Ok(text.clone()),
            ModelContentPart::File { file, policy } => {
                let part = file_part(provider, file, policy, images, counting, false, text_files)?;
                part.text()
                    .map(str::to_owned)
                    .ok_or_else(|| invalid("system image was not reduced to a fallback note"))
            }
            _ => Err(invalid("system messages cannot contain tool exchanges")),
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("\n"))
}

pub(super) fn file_part<'a>(
    provider: &ResolvedModel,
    file: &'a acyclic_harness::conversation::FileRef,
    policy: &'a FileProjectionPolicy,
    images: &mut dyn Iterator<Item = &'a [u8]>,
    counting: bool,
    allowed: bool,
    text_files: &mut crate::files::FileText,
) -> Result<Item<'a>> {
    if matches!(
        policy,
        FileProjectionPolicy::BoundedFull | FileProjectionPolicy::ProjectInstructions(_)
    ) {
        return Ok(text_part(
            provider.config.adapter,
            &text_files.project(file, policy)?,
        ));
    }
    if let FileProjectionPolicy::Native(policy) = policy
        && matches!(
            policy.intent,
            acyclic_harness::model::NativeMediaIntent::Image { .. }
        )
    {
        if allowed && crate::images::support(provider, file, policy)?.is_some() {
            let bytes = match images.next() {
                Some(bytes) => bytes,
                None if counting => &[],
                None => return Err(invalid("image file was not resolved before dispatch")),
            };
            return Ok(Item::Image(super::image::Image {
                adapter: provider.config.adapter,
                mime: file.descriptor().media_type(),
                policy,
                bytes,
            }));
        }
        let note = format!(
            "[Image {} omitted: {}/{} does not support this image format or message role]",
            file.display_name(),
            provider.model.provider,
            provider.model.name
        );
        return Ok(text_part(provider.config.adapter, &note));
    }
    Ok(text_part(
        provider.config.adapter,
        &file_text(file, policy)?,
    ))
}

fn file_text(
    file: &acyclic_harness::conversation::FileRef,
    policy: &FileProjectionPolicy,
) -> Result<String> {
    if !matches!(policy, FileProjectionPolicy::Reference) {
        return Err(invalid(
            "file bytes require an owner-mediated text projection before dispatch",
        ));
    }
    let value = serde_json::to_string(file).map_err(|e| invalid(e.to_string()))?;
    Ok(format!("[file: {value}]"))
}
