use crate::{Error, Result};

#[doc(hidden)]
pub fn frontmatter_end(prefix: &[u8]) -> Result<(usize, usize, usize)> {
    let mut offset = 0;
    let mut header_start = None;
    let mut header_end = None;
    for line in prefix.split_inclusive(|byte| *byte == b'\n') {
        let delimiter = line.strip_suffix(b"\n").unwrap_or(line);
        let delimiter = delimiter.strip_suffix(b"\r").unwrap_or(delimiter);
        if offset == 0 {
            if delimiter != b"---" || !line.ends_with(b"\n") {
                return Err(Error::Invalid(
                    "skill requires opening frontmatter delimiter".into(),
                ));
            }
            header_start = Some(line.len());
        } else if delimiter == b"---" && line.ends_with(b"\n") {
            header_end = Some((offset, offset + line.len()));
            break;
        }
        offset += line.len();
    }
    let (Some(start), Some((end, body))) = (header_start, header_end) else {
        return Err(Error::Invalid(
            "skill frontmatter is missing or exceeds prefix bound".into(),
        ));
    };
    Ok((start, end, body))
}

#[doc(hidden)]
pub fn parse_frontmatter<T: serde::de::DeserializeOwned>(prefix: &[u8]) -> Result<T> {
    let (start, end, _) = frontmatter_end(prefix)?;
    let header = prefix
        .get(start..end)
        .ok_or_else(|| Error::Invalid("invalid skill header".into()))?;
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_events: usize::MAX, max_nodes: usize::MAX, max_depth: usize::MAX,
            flow_nesting_limit: usize::MAX, max_documents: 1,
            max_aliases: 0, max_anchors: 0, max_merge_keys: 0,
            max_total_scalar_bytes: usize::MAX,
        },
        duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
        merge_keys: serde_saphyr::MergeKeyPolicy::Error,
        reject_unsupported_tags: true,
        emit_comments: false,
        with_snippet: false,
    };
    serde_saphyr::from_slice_with_options(header, options)
        .map_err(|error| Error::Invalid(format!("invalid frontmatter: {error}")))
}
