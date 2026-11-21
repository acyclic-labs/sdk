//! Kernel, format and native view codec contracts.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[path = "support/codec_association.rs"]
mod codec_association;
#[path = "support/codec_page_limit.rs"]
mod codec_page_limit;
#[path = "support/codec_properties.rs"]
mod codec_properties;
#[cfg(feature = "test-support")]
#[path = "support/export_manifest.rs"]
mod export_manifest;
#[cfg(feature = "test-support")]
#[path = "support/put_object.rs"]
mod object;
#[path = "support/portable_metadata.rs"]
mod portable_metadata;
#[cfg(feature = "test-support")]
use object::put_object;
#[cfg(feature = "test-support")]
#[path = "support/tree_objects.rs"]
mod tree_objects;

use acyclic_fs_native_contract::NativeRootIdentity;

#[cfg(feature = "test-support")]
#[path = "blob_io.rs"]
mod blob_io;
#[cfg(feature = "test-support")]
#[path = "directory_tree.rs"]
mod directory_tree;
#[cfg(feature = "test-support")]
#[path = "export_codec.rs"]
mod export_codec;
#[cfg(feature = "test-support")]
#[path = "extent_clone.rs"]
mod extent_clone;
#[path = "extent_codec.rs"]
mod extent_codec;
#[cfg(feature = "test-support")]
#[path = "generation_transfer.rs"]
mod generation_transfer;
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod materialization_bootstrap;
#[path = "metadata_codec.rs"]
mod metadata_codec;
#[path = "metadata_stamps.rs"]
mod metadata_stamps;
#[cfg(not(loom))]
mod mount_qualification;
#[path = "mount_routing.rs"]
mod mount_routing;
#[path = "native_checkpoint.rs"]
mod native_checkpoint;
#[path = "native_root_identity.rs"]
mod native_root_identity;
#[path = "operation_claim.rs"]
mod operation_claim;
#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod package_install;
#[cfg(not(loom))]
mod shared_storage_contract;
#[path = "tree_codec.rs"]
mod tree_codec;
