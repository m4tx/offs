use std::collections::HashMap;

use crate::remote_fs_client::fs::write_buffer::{WriteBuffer, WriteOperation};
use itertools::Itertools;

struct OpenFile {
    id: String,
    write_buffer: WriteBuffer,
}

impl OpenFile {
    fn new(id: String) -> Self {
        Self {
            id,
            write_buffer: WriteBuffer::new(),
        }
    }
}

pub struct OpenFileHandler {
    files: HashMap<u64, OpenFile>,
    next_fh: u64,
}

impl OpenFileHandler {
    pub fn new() -> Self {
        Self {
            files: Default::default(),
            next_fh: 1,
        }
    }

    pub fn open_file(&mut self, id: String) -> u64 {
        let open_file = OpenFile::new(id);
        let fh = self.next_fh;
        self.files.insert(fh, open_file);
        self.next_fh += 1;

        fh
    }

    pub fn close_file(&mut self, fh: u64) {
        self.files.remove(&fh);
    }

    pub fn get_file_handles(&self) -> Vec<u64> {
        self.files.keys().map(|x| *x).collect_vec()
    }

    #[must_use]
    pub fn write(&mut self, fh: u64, operation: WriteOperation) -> bool {
        self.files
            .get_mut(&fh)
            .unwrap()
            .write_buffer
            .write(operation)
    }

    #[must_use]
    pub fn flush(&mut self, fh: u64) -> (String, Vec<WriteOperation>) {
        let operations = self.files.get_mut(&fh).unwrap().write_buffer.flush();
        (self.files[&fh].id.clone(), operations)
    }
}
