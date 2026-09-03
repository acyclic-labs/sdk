use super::*;
use crate::foundation::Digest;
use crate::kernel::{FileKind, FilePayload};
use crate::path::PortablePath;

fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    Ok(NamespacePath::from_portable(
        &PortablePath::parse(value, limits)?,
        limits,
    )?)
}

fn object(kind: ObjectKind, byte: u8) -> ObjectId {
    ObjectId {
        kind,
        digest: Digest::from_bytes([byte; 32]),
    }
}

fn assert_rejected(
    mutation: Mutation,
    expected: MutationPlanError,
) -> Result<(), Box<dyn std::error::Error>> {
    let failure = MutationPlan::compile(
        vec![mutation],
        VolumeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("invalid mutation unexpectedly compiled")?;
    assert_eq!(failure.error, expected);
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn mixed_batch_compiles_shared_ancestors_once() -> Result<(), Box<dyn std::error::Error>> {
    let operations = vec![
        Mutation::Resize {
            path: path("/workspace/src/a")?,
            logical_bytes: 10,
        },
        Mutation::SetMetadata {
            path: path("/workspace/src/b")?,
            metadata: ObjectId {
                kind: ObjectKind::Metadata,
                digest: Digest::from_bytes([1; 32]),
            },
        },
        Mutation::Rename {
            source: path("/workspace/src/a")?,
            destination: path("/workspace/dst/a")?,
            replace: false,
        },
    ];
    let plan = MutationPlan::compile(
        operations.clone(),
        VolumeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(plan.operations(), operations);
    assert_eq!(plan.path_nodes(), 7);
    assert_eq!(plan.ordered_paths().count(), 4);
    assert_eq!(plan.work().items_examined, 18);
    assert_eq!(plan.work().allocation_operations, 1);
    assert_eq!(plan.work().bytes_copied, 0);
    Ok(())
}

#[test]
fn identity_only_batch_retains_no_path_plan_or_namespace_work()
-> Result<(), Box<dyn std::error::Error>> {
    let file_id = FileId::from_bytes([7; 16]);
    let plan = MutationPlan::compile(
        vec![Mutation::File {
            file_id,
            mutation: FileMutation::Resize { logical_bytes: 9 },
        }],
        VolumeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(plan.ordered_paths().count(), 0);
    assert_eq!(plan.retained_allocation_bytes(), 0);
    assert_eq!(plan.work(), WorkCounters::default());
    assert_eq!(plan.path_nodes(), 1);
    Ok(())
}

#[test]
fn malformed_ranges_and_root_mutation_fail_before_external_work()
-> Result<(), Box<dyn std::error::Error>> {
    let root = MutationPlan::compile(
        vec![Mutation::Resize {
            path: path("/")?,
            logical_bytes: 1,
        }],
        VolumeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("root mutation unexpectedly compiled")?;
    assert_eq!(root.error, MutationPlanError::RootMutation);
    assert_eq!(*root.work, WorkCounters::default());

    let range = MutationPlan::compile(
        vec![Mutation::ZeroRange {
            path: path("/file")?,
            offset: 0,
            length: 0,
            allocated: false,
            extend: false,
        }],
        VolumeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("empty range unexpectedly compiled")?;
    assert_eq!(range.error, MutationPlanError::EmptyRange);
    assert_eq!(*range.work, WorkCounters::default());
    Ok(())
}

#[test]
fn every_mutation_precondition_is_rejected_before_plan_work()
-> Result<(), Box<dyn std::error::Error>> {
    let regular = path("/regular")?;
    let other = path("/other")?;
    let root = path("/")?;
    let file_id = FileId::from_bytes([9; 16]);
    let invalid_record = FileRecord {
        file_id,
        kind: FileKind::Regular,
        link_count: 0,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Empty,
    };
    let namespace_cases = vec![
        (
            Mutation::Rename {
                source: regular.clone(),
                destination: root,
                replace: false,
            },
            MutationPlanError::RootMutation,
        ),
        (
            Mutation::Create {
                path: regular.clone(),
                record: invalid_record,
            },
            MutationPlanError::InvalidInitialRecord,
        ),
        (
            Mutation::Rename {
                source: regular.clone(),
                destination: regular.clone(),
                replace: false,
            },
            MutationPlanError::SameSourceAndDestination,
        ),
        (
            Mutation::Link {
                source: other.clone(),
                destination: other.clone(),
            },
            MutationPlanError::SameSourceAndDestination,
        ),
    ];
    for (mutation, expected) in namespace_cases {
        assert_rejected(mutation, expected)?;
    }

    let content_cases = vec![
        (
            Mutation::Write {
                path: regular.clone(),
                offset: 0,
                length: 1,
                content: object(ObjectKind::Metadata, 2),
                content_offset: 0,
            },
            MutationPlanError::WrongContentKind,
        ),
        (
            Mutation::Write {
                path: regular.clone(),
                offset: 0,
                length: 2,
                content: object(ObjectKind::Blob, 3),
                content_offset: u64::MAX,
            },
            MutationPlanError::RangeOverflow,
        ),
        (
            Mutation::SetMetadata {
                path: regular.clone(),
                metadata: object(ObjectKind::Blob, 4),
            },
            MutationPlanError::WrongMetadataKind,
        ),
        (
            Mutation::File {
                file_id,
                mutation: FileMutation::SetMetadata {
                    metadata: object(ObjectKind::Blob, 5),
                },
            },
            MutationPlanError::WrongMetadataKind,
        ),
        (
            Mutation::CloneRange {
                source: regular,
                source_offset: u64::MAX,
                destination: other,
                destination_offset: 0,
                length: 2,
            },
            MutationPlanError::RangeOverflow,
        ),
    ];
    for (mutation, expected) in content_cases {
        assert_rejected(mutation, expected)?;
    }
    Ok(())
}

#[test]
fn path_use_count_deduplicates_only_identical_endpoints() -> Result<(), Box<dyn std::error::Error>>
{
    let first = path("/first")?;
    let second = path("/second")?;
    let operations = [
        Mutation::Remove {
            path: first.clone(),
            expected_file_id: MetadataField::Unavailable,
        },
        Mutation::Rename {
            source: first.clone(),
            destination: first,
            replace: false,
        },
        Mutation::Rename {
            source: second.clone(),
            destination: path("/third")?,
            replace: false,
        },
    ];
    assert_eq!(path_use_count(&operations), Some(4));
    assert_eq!(path_use_count(&[]), Some(0));
    Ok(())
}

#[test]
fn operation_bound_is_checked_before_plan_allocation() -> Result<(), Box<dyn std::error::Error>> {
    let limits = VolumeLimits {
        maximum_mutations_per_batch: 1,
        ..VolumeLimits::default()
    };
    let failure = MutationPlan::compile(
        vec![
            Mutation::Resize {
                path: path("/a")?,
                logical_bytes: 1,
            },
            Mutation::Resize {
                path: path("/b")?,
                logical_bytes: 1,
            },
        ],
        limits,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("oversized batch unexpectedly compiled")?;
    assert_eq!(failure.error, MutationPlanError::TooManyMutations);
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn flat_plan_memory_is_admitted_before_allocator_access() -> Result<(), Box<dyn std::error::Error>>
{
    let operation = Mutation::Resize {
        path: path("/large")?,
        logical_bytes: 1,
    };
    let mut operation_budget = WorkBudget::UNBOUNDED;
    operation_budget.allocation_operations = 0;
    let operation_failure = MutationPlan::compile(
        vec![operation.clone()],
        VolumeLimits::default(),
        operation_budget,
    )
    .err()
    .ok_or("allocation operation unexpectedly admitted")?;
    assert_eq!(*operation_failure.work, WorkCounters::default());

    let mut peak_budget = WorkBudget::UNBOUNDED;
    peak_budget.peak_allocation_bytes = 0;
    let peak_failure = MutationPlan::compile(vec![operation], VolumeLimits::default(), peak_budget)
        .err()
        .ok_or("allocation peak unexpectedly admitted")?;
    assert_eq!(*peak_failure.work, WorkCounters::default());
    Ok(())
}
