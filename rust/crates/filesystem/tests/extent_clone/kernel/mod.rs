mod blob;
mod extent_clone;
mod file_read;
mod range;

#[path = "../../support/blob_objects.rs"]
mod blob_objects;
#[path = "../../support/extent_objects.rs"]
mod extent_objects;

use extent_objects::put_object;
