//! Binding names for filesystem engine types.

acyclic_fs_format::public_names! {
    crate::kernel::NameEncoding {
        Utf8 => "utf8",
        PosixBytes => "posix-bytes",
        WindowsUtf16Le => "windows-utf16le",
    }
    crate::speculation::ResidencyReason {
        DirectorySuccessor => "directory-successor",
        SequentialRange => "sequential-range",
        MetadataSuccessor => "metadata-successor",
        ConsumerHint => "consumer-hint",
    }
    crate::speculation::ResidencyRejection {
        WrongVolume => "wrong-volume",
        StaleGeneration => "stale-generation",
        InvalidRequest => "invalid-request",
        DuplicateObject => "duplicate-object",
        DuplicateOperation => "duplicate-operation",
        CostBudget => "cost-budget",
        LowUsefulness => "low-usefulness",
    }
    crate::speculation::StorageTier {
        ProcessMemory => "process-memory",
        NodeLocal => "node-local",
        SharedCache => "shared-cache",
        DurableOrigin => "durable-origin",
    }
    crate::speculation::PromotionRejection {
        WrongVolume => "wrong-volume",
        StaleGeneration => "stale-generation",
        InvalidRequest => "invalid-request",
        MissingSource => "missing-source",
        DuplicateObject => "duplicate-object",
        DuplicateOperation => "duplicate-operation",
        NoDestination => "no-destination",
        LowUsefulness => "low-usefulness",
    }
}
