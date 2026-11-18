mod file_table;
mod list;
mod read;
mod tree_mutation;

#[path = "../../support/file_table_objects.rs"]
mod file_table_objects;
#[path = "../../support/put_object.rs"]
mod object;
#[path = "../../support/tree_entries.rs"]
mod tree_entries;
#[path = "../../support/tree_objects.rs"]
mod tree_objects;

use object::put_object;
