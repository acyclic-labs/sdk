use super::*;
use crate::conversation::{FileDescriptor, VolumeOwner, VolumeRef};
use crate::executor::content::validate_model_content_scope;
use crate::resources::ProviderRef;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // A readable summary never requires exposing any host-journal payload.
    #[test]
    fn summary_read_authority_is_independent_of_the_host_journal(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let volume = |name: &str| VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            name.into(), VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([1; 16])), &mut None,
        ).unwrap();
        let private = volume("private");
        let journal = volume("journal");
        let scope = RuntimeScope::new(crate::Capabilities::new([
            private.capability(crate::conversation::VolumeOperation::Read, &mut None).unwrap(),
            journal.directory_read_capability("inputs", &mut None).unwrap(),
        ]), Limits::default()).unwrap();
        let content = |volume| ModelContent::Part(crate::model::ModelContentPart::File {
            file: FileRef::new(volume, "summaries/turn/0.txt".into(), "1".into(),
                FileDescriptor::from_bytes(&bytes, "text/plain", &mut None).unwrap(),
                "summary.txt".into(), &mut None).unwrap(),
            policy: crate::model::FileProjectionPolicy::BoundedFull,
        });
        let private = content(private);
        let journal = content(journal);
        prop_assert!(validate_model_content_scope(&private, Limits::default(), Some(&scope), None).is_ok());
        prop_assert!(matches!(validate_model_content_scope(&journal, Limits::default(), Some(&scope), None),
            Err(Error::Unauthorized(_))));
        // Broken control: omitting the task scope admits that same journal file.
        prop_assert!(validate_model_content_scope(&journal, Limits::default(), None, None).is_ok());
    }
}
