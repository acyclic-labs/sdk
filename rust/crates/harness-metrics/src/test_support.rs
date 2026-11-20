//! Shared readers for the single-row Arrow batches these projections emit.

use crate::Batch;
use arrow_array::{Array, StringArray, UInt64Array};

pub(crate) fn text<'a>(batch: &'a Batch, name: &str) -> &'a str {
    batch
        .data
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
}

pub(crate) fn number(batch: &Batch, name: &str) -> u64 {
    batch
        .data
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}
