use super::*;

pub(super) fn exact_mutation_regions(
    mutation: &Mutation,
    first: Option<FileRecord>,
    second: Option<FileRecord>,
) -> [Option<(Option<FileRecord>, DependencyRegion)>; 4] {
    let mut regions = [None, None, None, None];
    match mutation {
        Mutation::Create { .. } | Mutation::Link { .. } => {}
        Mutation::Restore { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Remove { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Rename { .. } => {
            regions[0] =
                second.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::SetMetadata { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::Metadata(record.file_id)));
        }
        Mutation::Write { offset, length, .. } | Mutation::ZeroRange { offset, length, .. } => {
            let [content, file_length] = regular_range_regions(first, *offset, *length);
            regions[0] = content.map(|region| (first, region));
            regions[1] = file_length.map(|region| (first, region));
        }
        Mutation::ValidateRegular { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::Preallocate { .. } => {
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileLength(record.file_id)));
        }
        Mutation::Resize { .. } => {
            // Resize can discard an arbitrarily large suffix. Until a compact
            regions[0] =
                first.map(|record| (Some(record), DependencyRegion::FileRecord(record.file_id)));
        }
        Mutation::CloneRange {
            source_offset,
            destination_offset,
            length,
            ..
        } => {
            let [source_content, source_length] =
                regular_range_regions(first, *source_offset, *length);
            let [destination_content, destination_length] =
                regular_range_regions(second, *destination_offset, *length);
            regions[0] = source_content.map(|region| (first, region));
            regions[1] = source_length.map(|region| (first, region));
            regions[2] = destination_content.map(|region| (second, region));
            regions[3] = destination_length.map(|region| (second, region));
        }
        Mutation::File { file_id, mutation } => match mutation {
            FileMutation::SetMetadata { .. } => {
                regions[0] = Some((first, DependencyRegion::Metadata(*file_id)));
            }
            FileMutation::Write { offset, length, .. }
            | FileMutation::ZeroRange { offset, length, .. } => {
                let [content, file_length] = regular_range_regions(first, *offset, *length);
                regions[0] = content.map(|region| (first, region));
                regions[1] = file_length.map(|region| (first, region));
            }
            FileMutation::Preallocate { .. } => {
                regions[0] = Some((first, DependencyRegion::FileLength(*file_id)));
            }
            FileMutation::ReplaceRecord { .. }
            | FileMutation::ValidateRegular
            | FileMutation::Resize { .. } => {
                regions[0] = Some((first, DependencyRegion::FileRecord(*file_id)));
            }
        },
        Mutation::CloneFileRange {
            source_file_id,
            source_offset,
            destination_file_id,
            destination_offset,
            length,
        } => {
            let [source_content, source_length] =
                regular_range_regions(first, *source_offset, *length);
            let [destination_content, destination_length] =
                regular_range_regions(second, *destination_offset, *length);
            regions[0] = source_content.map(|region| (first, region));
            regions[1] = source_length.map(|region| (first, region));
            regions[2] = destination_content.map(|region| (second, region));
            regions[3] = destination_length.map(|region| (second, region));
            debug_assert!(first.is_none_or(|record| record.file_id == *source_file_id));
            debug_assert!(second.is_none_or(|record| record.file_id == *destination_file_id));
        }
    }
    regions
}
