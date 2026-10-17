use super::*;
use serde::ser::SerializeSeq;

struct Canonical<T>(T);

impl<T: Serialize> Serialize for Canonical<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut value = serde_json::to_value(&self.0).map_err(serde::ser::Error::custom)?;
        value.sort_all_objects();
        value.serialize(serializer)
    }
}

struct Events<'a>(&'a [Event]);

impl Serialize for Events<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for event in self.0 {
            sequence.serialize_element(&Canonical(event))?;
        }
        sequence.end()
    }
}

pub(super) fn without_work(snapshot: &Snapshot) -> Result<[u8; 32]> {
    crate::executor::json::serialized_json_digest(&(
        snapshot.format_version,
        Canonical(&snapshot.authority),
        snapshot.revision,
        Events(&snapshot.events),
        Canonical(&snapshot.projection),
    ))
}
