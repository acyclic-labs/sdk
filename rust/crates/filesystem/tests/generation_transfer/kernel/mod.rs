mod checkpoint;
mod export;
mod transfer;

#[path = "../../support/file_table_objects.rs"]
mod file_table_objects;
#[path = "../../support/generation_objects.rs"]
mod generation_objects;
#[path = "../../support/put_object.rs"]
mod object;
#[path = "../../support/tree_objects.rs"]
mod tree_objects;

use object::put_object;
