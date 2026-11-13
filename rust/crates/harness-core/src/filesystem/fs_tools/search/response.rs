use serde::Serialize;
#[derive(Serialize)]
pub(super) struct Reply<T> {
    pub(super) result: T,
}
#[derive(Serialize)]
pub(super) struct SearchResult<'a> {
    pub(super) kind: &'static str,
    pub(super) matches: Rows<'a>,
    pub(super) work: u32,
    pub(super) next: Option<&'a str>,
}
pub(super) struct Rows<'a>(
    pub(super)  &'a [acyclic_stream::search::Match<
        acyclic_fs::kernel::Retained<crate::conversation::FileRef>,
    >],
);
impl Serialize for Rows<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut rows = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            rows.serialize_element(&Row(row))?;
        }
        rows.end()
    }
}
struct Row<'a>(
    &'a acyclic_stream::search::Match<acyclic_fs::kernel::Retained<crate::conversation::FileRef>>,
);
impl Serialize for Row<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut commit = [0; 64];
        hex::encode_to_slice(self.0.record.commit_id.as_bytes(), &mut commit)
            .map_err(serde::ser::Error::custom)?;
        let commit = std::str::from_utf8(&commit).map_err(serde::ser::Error::custom)?;
        let mut row = serializer.serialize_struct("SearchMatch", 5)?;
        row.serialize_field("path", self.0.path.as_str())?;
        row.serialize_field("sequence", &self.0.record.sequence)?;
        row.serialize_field("commit", commit)?;
        row.serialize_field("origin", self.0.record.origin.as_str())?;
        row.serialize_field("file", &self.0.source.as_deref())?;
        row.end()
    }
}
