use super::*;

#[path = "../support/archive.rs"]
mod fixture_archive;
use fixture_archive::encode;
pub(super) use fixture_archive::pack;

proptest::proptest! {
    #[test]
    fn fixture_archive_members_round_trip_without_host_tar(
        members in proptest::collection::btree_map(
            "[a-z][a-z0-9_]{0,8}(/[a-z][a-z0-9_]{0,8}){0,2}",
            proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512), 1..8)
    ) {
        super::runtime::time_control();
        let bytes = encode("fixture-1.0.0", &members);
        proptest::prop_assert_eq!(
            sdk_generation::historical::decode_archive_members(&bytes, "fixture", "1.0.0").unwrap(),
            members);
    }
}
