use super::*;
use crate::model::ModelContent;

impl TaskContext {
    pub(crate) async fn verify_model_contents<'a>(
        &self,
        contents: impl Iterator<Item = &'a ModelContent> + Clone + acyclic_stream::ProviderTask,
    ) -> Result<()> {
        let limits = self.scope.limits();
        for content in contents.clone() {
            content.validate_limits(limits, &mut None)?;
            for file in content.file_refs() {
                limits.validate_file(file, &mut None)?;
                if !read_granted(self.scope.grants(), file)? {
                    return Err(Error::Unauthorized(
                        "task scope cannot project this file".into(),
                    ));
                }
            }
        }
        let configurations = contents
            .clone()
            .flat_map(ModelContent::native_configurations)
            .map(|binding| {
                let proof = binding.verify_original(
                    self.scope.extensions.as_ref(),
                    self.scope.extension_schemas.as_ref(),
                    self.scope.extension_runtime.as_deref(),
                    None,
                )?;
                Ok((binding, proof))
            })
            .collect::<Result<Vec<_>>>()?;
        for (binding, (schemas, expected)) in &configurations {
            let content = self
                .content_binding(Some(binding.configuration.content.volume()))
                .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
            let bytes = content
                .reader
                .read(
                    &binding.configuration.content,
                    binding.configuration.content.descriptor().byte_length(),
                    None,
                    None,
                )?
                .await?;
            schemas.verify_configuration_binding(
                &binding.configuration,
                *expected,
                &bytes,
                limits,
                None,
            )?;
        }
        for file in contents.flat_map(ModelContent::file_refs) {
            if !configurations
                .iter()
                .any(|(binding, _)| &binding.configuration.content == file)
            {
                self.content_binding(Some(file.volume()))
                    .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?
                    .reader
                    .verify(file, file.descriptor().byte_length(), None)?
                    .await?;
            }
        }
        Ok(())
    }
}
