use super::*;

impl StagedArchive {
    pub(in crate::historical) fn assert_storage_contract(
        &self,
        original: &BTreeMap<String, Vec<u8>>,
    ) {
        // A real directory observation rejects restored per-member staging,
        // independently of the logical member index.
        assert_eq!(fs::read_dir(self.storage.path()).unwrap().count(), 1);
        assert_eq!(
            fs::metadata(self.storage.path().join("payload"))
                .unwrap()
                .len(),
            original
                .values()
                .map(|bytes| bytes.len() as u64)
                .sum::<u64>()
        );
        // Open every range before reading, in reverse archive order. Sharing a
        // seek cursor or omitting the range bound changes these original bytes.
        let readers = original
            .iter()
            .rev()
            .map(|(name, bytes)| (self.reader(&self.files[name]).unwrap(), bytes))
            .collect::<Vec<_>>();
        for (mut reader, expected) in readers {
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).unwrap();
            assert_eq!(&actual, expected);
            assert_eq!(reader.limit(), 0);
        }
    }
}
