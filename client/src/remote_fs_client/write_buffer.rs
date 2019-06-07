use std::collections::BTreeSet;

const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8 MiB

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct WriteOperation {
    pub id: String,
    pub offset: usize,
    pub data: Vec<u8>,
}

impl WriteOperation {
    pub fn new(id: String, offset: usize, data: Vec<u8>) -> Self {
        Self { id, offset, data }
    }
}

pub struct WriteBuffer {
    size: usize,
    operations: BTreeSet<WriteOperation>,
}

impl WriteBuffer {
    pub fn new() -> Self {
        Self {
            size: 0,
            operations: BTreeSet::new(),
        }
    }

    pub fn add_write_op(&mut self, write_operation: WriteOperation) {
        self.size += write_operation.data.len();
        self.operations.insert(write_operation);
    }

    pub fn is_full(&self) -> bool {
        self.size >= BUFFER_SIZE
    }

    pub fn flush(&mut self) -> Vec<WriteOperation> {
        let mut result = Vec::new();

        let mut old_operations = BTreeSet::new();
        std::mem::swap(&mut self.operations, &mut old_operations);

        for mut op in old_operations.into_iter() {
            if result.len() == 0 {
                result.push(op);
                continue;
            }

            let last = result.last_mut().unwrap();
            if last.id == op.id && last.offset + last.data.len() == op.offset {
                last.data.append(&mut op.data);
            } else {
                result.push(op);
            }
        }

        self.size = 0;

        result
    }
}
