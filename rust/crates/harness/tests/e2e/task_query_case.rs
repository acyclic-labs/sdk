include!("../support/schema/task_query.rs");

use acyclic_harness::task_volume::{FileKey, Write};

impl File {
    pub fn write(&self) -> acyclic_harness::task_volume::Result<Write> {
        let mut file = Write::new(FileKey::parse(&self.path)?, self.text.as_bytes().to_vec());
        file.channel = self.channel;
        Ok(file)
    }
}
