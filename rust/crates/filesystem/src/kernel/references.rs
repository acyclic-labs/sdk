//! Immediate authenticated references, shared by immutable storage compositions.
use super::*;
use crate::{FsError, ObjectId, ObjectKind, WorkError};

pub(crate) fn visit_object_references(
    id: ObjectId,
    bytes: &[u8],
    work: &mut KernelAccounting<'_>,
    mut visit: impl FnMut(ObjectId) -> Result<(), FsError>,
) -> Result<(), FsError> {
    let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    let limits = DecodeLimits {
        maximum_object_bytes: bytes.len() as u64,
        maximum_page_bytes: length,
        maximum_name_bytes: length,
        maximum_page_items: length,
        maximum_page_height: u16::MAX,
        maximum_visited_pages: length,
    };
    macro_rules! decode {
        ($decoder:path, $body:expr) => {{
            let mut allocated = 0u64;
            let (result, allocation) = {
                let (value, allocation) = file_read::decode_owned(work, |observe| {
                    $decoder(bytes, limits, &mut |step| {
                        match &step {
                            codec::ReadAllocationStep::Allocate { bytes, .. } => {
                                allocated =
                                    allocated.checked_add(*bytes).ok_or(WorkError::Overflow)?
                            }
                            codec::ReadAllocationStep::Capacity {
                                admitted_bytes,
                                actual_bytes,
                            } => {
                                allocated = allocated
                                    .checked_sub(*admitted_bytes)
                                    .and_then(|n| n.checked_add(*actual_bytes))
                                    .ok_or(WorkError::Overflow)?
                            }
                            _ => {}
                        }
                        observe(step)
                    })
                })?;
                (($body)(&value, &mut visit), allocation)
            };
            persistent_point::retire_allocation(allocation, allocated, work)?;
            result
        }};
    }
    match id.kind {
        ObjectKind::BlobChunk => Ok(()),
        ObjectKind::GenerationRoot => decode!(
            generation::decode_generation_root,
            |root: &GenerationRoot, visit: &mut dyn FnMut(ObjectId) -> Result<(), FsError>| {
                visit(root.file_table)
            }
        ),
        ObjectKind::Blob => decode!(blob::decode_blob_page, |page: &blob::BlobPage,
                                                             visit: &mut dyn FnMut(
            ObjectId,
        )
            -> Result<
            (),
            FsError,
        >| {
            match &page.node {
                blob::BlobNode::Leaf(items) => {
                    for item in items {
                        visit(item.chunk)?;
                    }
                }
                blob::BlobNode::Internal(items) => {
                    for item in items {
                        visit(item.page)?;
                    }
                }
            }
            Ok(())
        }),
        ObjectKind::TreePage => {
            decode!(tree::decode_tree_page, |page: &TreePage,
                                             visit: &mut dyn FnMut(
                ObjectId,
            )
                -> Result<
                (),
                FsError,
            >| {
                if let TreePage::Internal(items) = page {
                    for item in items {
                        visit(item.page)?;
                    }
                }
                Ok(())
            })
        }
        ObjectKind::ExtentPage => decode!(
            extent::decode_extent_page,
            |page: &ExtentPage, visit: &mut dyn FnMut(ObjectId) -> Result<(), FsError>| {
                match page {
                    ExtentPage::Leaf(items) => {
                        for item in items {
                            if let ExtentKind::Content { object, .. } = item.kind {
                                visit(object)?;
                            }
                        }
                    }
                    ExtentPage::Internal(items) => {
                        for item in items {
                            visit(item.page)?;
                        }
                    }
                }
                Ok(())
            }
        ),
        ObjectKind::AttributePage => decode!(
            attribute::decode_attribute_page,
            |page: &attribute::AttributePage,
             visit: &mut dyn FnMut(ObjectId) -> Result<(), FsError>| {
                match page {
                    attribute::AttributePage::Leaf(items) => {
                        for item in items {
                            visit(item.value)?;
                        }
                    }
                    attribute::AttributePage::Internal(items) => {
                        for item in items {
                            visit(item.page)?;
                        }
                    }
                }
                Ok(())
            }
        ),
        ObjectKind::Metadata => decode!(
            metadata::decode_file_metadata,
            |metadata: &FileMetadata, visit: &mut dyn FnMut(ObjectId) -> Result<(), FsError>| {
                for field in [
                    metadata.named_attributes,
                    metadata.acl,
                    metadata.security_descriptor,
                ] {
                    if let MetadataField::Value(id) = field {
                        visit(id)?;
                    }
                }
                Ok(())
            }
        ),
        ObjectKind::FileTablePage => decode!(
            file_table::decode_file_table_page,
            |page: &FileTablePage, visit: &mut dyn FnMut(ObjectId) -> Result<(), FsError>| {
                match page {
                    FileTablePage::Internal(items) => {
                        for item in items {
                            visit(item.page)?;
                        }
                    }
                    FileTablePage::Leaf(items) => {
                        for item in items {
                            visit(item.metadata)?;
                            match item.payload {
                                FilePayload::Regular { extents, .. } => visit(extents)?,
                                FilePayload::Directory { entries } => visit(entries)?,
                                FilePayload::SymbolicLink { target, .. } => visit(target)?,
                                FilePayload::ReparsePoint { payload, .. } => visit(payload)?,
                                _ => {}
                            }
                        }
                    }
                }
                Ok(())
            }
        ),
    }
}
