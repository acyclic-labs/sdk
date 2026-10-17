use super::*;

fn encode(package: &str, members: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
    let mut archive = tar::Builder::new(Vec::new());
    for (name, bytes) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_cksum();
        archive
            .append_data(&mut header, format!("{package}/{name}"), bytes.as_slice())
            .unwrap();
    }
    archive.into_inner().unwrap()
}

pub(super) fn pack(source: &Path, destination: &Path) {
    fn files(root: &Path, directory: &Path, members: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                files(root, &path, members);
            } else {
                assert!(entry.file_type().unwrap().is_file());
                let name = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace('\\', "/");
                assert!(members.insert(name, fs::read(path).unwrap()).is_none());
            }
        }
    }
    let mut members = BTreeMap::new();
    files(source, source, &mut members);
    fs::write(
        destination,
        encode(source.file_name().unwrap().to_str().unwrap(), &members),
    )
    .unwrap();
}

proptest::proptest! {
    #[test]
    fn fixture_archive_members_round_trip_without_host_tar(
        members in proptest::collection::btree_map(
            "[a-z][a-z0-9_]{0,8}(/[a-z][a-z0-9_]{0,8}){0,2}",
            proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512), 1..8)
    ) {
        crate::runtime::time_control();
        let bytes = encode("fixture-1.0.0", &members);
        proptest::prop_assert_eq!(
            sdk_generation::historical::decode_archive_members(&bytes, "fixture", "1.0.0").unwrap(),
            members);
    }
}
