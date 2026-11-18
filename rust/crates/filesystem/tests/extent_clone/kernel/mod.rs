mod blob;
mod extent_clone;
mod file_read;
mod range;

#[path = "../../support/extent_objects.rs"]
mod extent_objects;
#[path = "../../support/memory_objects.rs"]
mod memory;

#[path = "../../support/blob_objects.rs"]
mod blob_objects;

use extent_objects::put_object;
