mod attribute_list;
mod file_table;
mod list;
mod read;
mod tree_mutation;

#[path = "../../support/attribute_entries.rs"]
mod attribute_entries;
#[path = "../../support/attribute_objects.rs"]
mod attribute_objects;
#[path = "../../support/file_table_objects.rs"]
mod file_table_objects;
#[path = "../../support/put_object.rs"]
mod object;
#[path = "../../support/tree_entries.rs"]
mod tree_entries;
#[path = "../../support/tree_objects.rs"]
mod tree_objects;

use object::put_object;
