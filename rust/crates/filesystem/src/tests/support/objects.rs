#[path = "../../../tests/support/extent_objects.rs"]
mod extent_objects;
pub(crate) use extent_objects::{put_extent_page, put_object};

#[path = "../../../tests/support/generation_objects.rs"]
mod generation_objects;
pub(crate) use generation_objects::{put_file_metadata, put_generation_root};

#[path = "../../../tests/support/file_table_objects.rs"]
mod file_table_objects;
pub(crate) use file_table_objects::put_file_table_page;

#[path = "../../../tests/support/tree_objects.rs"]
mod tree_objects;
pub(crate) use tree_objects::put_tree_page;

#[path = "../../../tests/support/attribute_objects.rs"]
mod attribute_objects;
pub(crate) use attribute_objects::put_attribute_page;

#[path = "../../../tests/support/blob_objects.rs"]
mod blob_objects;
pub(crate) use blob_objects::put_blob_page;
