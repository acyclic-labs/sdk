use super::*;

pub(in crate::v1::memory) fn bodies(
    body: &StoredBody,
    publication: &mut Publication,
) -> Result<Vec<BodyRecord>, NativeObjectsError> {
    fn ordered(
        body: &StoredBody,
        out: &mut Vec<BodyRecord>,
        publication: &mut Publication,
    ) -> Result<(), NativeObjectsError> {
        #[cfg(all(test, feature = "local"))]
        super::super::persistence::work::encoded();
        match body {
            #[cfg(feature = "local")]
            StoredBody::Local {
                digest,
                length,
                location,
                ..
            } => {
                let (segment, offset, journal) = match location {
                    LocalBodyLocation::Segment { id, offset } => {
                        (publication.bytes(id)?, *offset, false)
                    }
                    LocalBodyLocation::Journal { offset } => (Vec::new(), *offset, true),
                };
                let digest = publication.bytes(digest)?;
                publication.push(
                    out,
                    BodyRecord {
                        segment,
                        digest,
                        length: *length as u64,
                        offset,
                        journal,
                    },
                )?;
                Ok(())
            }
            #[cfg(feature = "s3")]
            StoredBody::Remote(value) => {
                let record = BodyRecord {
                    segment: publication.bytes(&value.id)?,
                    digest: publication.bytes(&value.id)?,
                    length: value.length as u64,
                    offset: 0,
                    journal: false,
                };
                publication.push(out, record)
            }
            StoredBody::Composite { parts, .. } => {
                for part in parts.value.iter() {
                    ordered(part, out, publication)?;
                }
                Ok(())
            }
            _ => Err(Error::from(Unavailable).into()),
        }
    }
    let mut output = Vec::new();
    ordered(body, &mut output, publication)?;
    Ok(output)
}
