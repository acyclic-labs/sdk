use super::*;

/// Lazily routes immutable Objects refs to separately registered owner bindings.
#[derive(Default)]
pub struct ObjectContentMountResolver {
    stores: BTreeMap<String, Arc<ObjectContentStore>>,
}

impl ObjectContentMountResolver {
    /// Creates a resolver from independently authenticated bucket bindings.
    pub fn new(stores: impl IntoIterator<Item = Arc<ObjectContentStore>>) -> Result<Self> {
        let mut resolver = Self::default();
        for store in stores {
            store.volume.validate(&mut None)?;
            let key = store.volume.capability(VolumeOperation::Read, &mut None)?;
            if resolver.stores.contains_key(&key) {
                return Err(Error::Invalid(
                    "Objects content volume is registered twice".into(),
                ));
            }
            resolver.stores.insert(key, store);
        }
        Ok(resolver)
    }
}

impl crate::conversation::ContentMountResolver for ObjectContentMountResolver {
    fn mount<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> BoxFuture<'a, Result<(ContentGrant, Arc<dyn ContentResidencyVerifier>)>> {
        Box::pin(async move {
            reference.validate(&mut None)?;
            let store = self
                .stores
                .get(
                    &reference
                        .volume()
                        .capability(VolumeOperation::Read, &mut None)?,
                )
                .cloned()
                .ok_or_else(|| {
                    Error::Unsupported("Objects content volume is not registered".into())
                })?;
            // Authenticate the current scope against this exact content reference before returning either the grant or the owner reader.
            let grant = ContentGrant::verify_read(&store.verifier, &store.scope, reference)?;
            grant.require_file_read(reference, None)?;
            Ok((grant, store as Arc<dyn ContentResidencyVerifier>))
        })
    }
}
