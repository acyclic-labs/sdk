//! The immutable selection transported by an opaque file-tool version token.
use super::*;

#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileVersion {
    volume: VolumeRef,
    #[serde(deserialize_with = "deserialize_path")]
    #[heap_layout(scalar)]
    path: String,
    #[serde(deserialize_with = "deserialize_version")]
    #[heap_layout(scalar)]
    version: Box<str>,
    descriptor: FileDescriptor,
}

impl FileVersion {
    /// Native callers reserve these additional bytes before constructing the
    /// ordinary reference. A display name never participates in selection.
    pub(crate) fn display_bytes(&self) -> usize {
        self.display_name().len()
    }

    fn display_name(&self) -> &str {
        self.path.rsplit('/').next().unwrap_or(&self.path)
    }

    pub(crate) fn into_file(self) -> FileRef {
        let display_name = self.display_name().to_owned();
        FileRef {
            volume: self.volume,
            path: self.path,
            version: self.version,
            descriptor: self.descriptor,
            display_name,
            publication: None,
        }
    }
}
