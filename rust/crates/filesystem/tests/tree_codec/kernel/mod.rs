mod tree;
mod types;

mod read;

#[path = "../../support/memory_objects.rs"]
mod memory;
#[path = "../../support/put_object.rs"]
mod object;
#[path = "../../support/tree_objects.rs"]
mod tree_objects;

use object::put_object;
