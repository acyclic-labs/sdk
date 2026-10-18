use super::*;
use acyclic_stream::OwnedBytes;
pub(super) struct Reader;
impl ContentResidencyVerifier for Reader {
    fn read<'a>(
        &'a self,
        _: &'a crate::conversation::FileRef,
        _: u64,
        _: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(None, || async {
            Ok(OwnedBytes::from(vec![1]))
        })
    }
}
pub(super) fn image(revision: u8) -> crate::conversation::FileRef {
    use crate::conversation::{FileDescriptor, FileRef};
    let fixture: FileRef =
        serde_json::from_str(include_str!("../../../fixtures/v2/file-ref.json")).unwrap();
    FileRef::new(
        fixture.volume().clone(),
        "attachments/image.png".into(),
        format!("revision-{revision}"),
        FileDescriptor::from_bytes(&[1], "image/png", &mut None).unwrap(),
        "image".into(),
        &mut None,
    )
    .unwrap()
}
