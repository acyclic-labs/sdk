use super::*;
use crate::{
    context::ContextManifest,
    conversation::{
        BoundedContentFuture, ContentIo, ContentResidencyVerifier, FileDescriptor, FileRef, Limits,
        VolumeClass, VolumeOwner, VolumeRef,
    },
    resources::ProviderRef,
};

struct ManifestBytes(Vec<u8>);

impl ContentResidencyVerifier for ManifestBytes {
    fn read<'a>(
        &'a self,
        _: &'a FileRef,
        _: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        BoundedContentFuture::new(io, || async { Ok(self.0.clone().into()) })
    }
}

// The same detector must accept ownership transfer and reject the former
// clone-before-pin path. Pointer identity proves no registry-map copy occurs,
// independently of allocator size classes or tool payload sizes.
proptest! {
    #[test]
    fn binding_consumes_registry_without_copying_maps(count in 0usize..32, keep in any::<bool>()) {
        let mut tools = ToolRegistry::default();
        let template = checked(prototype())?;
        for index in 0..count {
            let mut tool = template.clone();
            tool.definition.name = format!("tool.{index}");
            checked(tools.register((tool, None), &mut None, None))?;
        }
        let mut manifest = checked(ContextManifest::new(Default::default(), Vec::new(), &tools))?;
        if !keep { manifest.tools.clear(); }
        let bytes = checked(manifest.encode(Limits::default()))?;
        let provider = checked(ProviderRef::new("test".into(), "filesystem".into(), "1".into(), &mut None))?;
        let volume = checked(VolumeRef::new(provider, "v".into(), VolumeClass::Project,
            VolumeOwner::Project("test".into()), &mut None))?;
        let descriptor = checked(FileDescriptor::from_bytes(&bytes, "application/json", &mut None))?;
        let reference = checked(FileRef::new(volume, "manifest.json".into(), "1".into(),
            descriptor, "manifest.json".into(), &mut None))?;
        let reader = ManifestBytes(bytes);
        for broken in [true, false] {
            let input = if broken { tools.clone() } else { std::mem::take(&mut tools) };
            let before = Arc::as_ptr(&input.data);
            let (bound, _) = checked(futures::executor::block_on(ContextManifest::bind(
                reference.clone(), &reader, input, Limits::default(),
            )))?;
            prop_assert_eq!(Arc::as_ptr(&bound.data) == before, !broken);
            prop_assert_eq!(checked(bound.definitions(None))?, manifest.tools.clone());
        }
    }
}
