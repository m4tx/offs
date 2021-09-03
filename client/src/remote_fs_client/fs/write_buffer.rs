use std::collections::BTreeSet;

const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8 MiB

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct WriteOperation {
    pub offset: usize,
    pub data: Vec<u8>,
}

impl WriteOperation {
    pub fn new(offset: usize, data: Vec<u8>) -> Self {
        Self { offset, data }
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

    #[must_use]
    pub fn write(&mut self, write_operation: WriteOperation) -> bool {
        self.size += write_operation.data.len();
        self.operations.insert(write_operation);

        self.is_full()
    }

    fn is_full(&self) -> bool {
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
            if last.offset + last.data.len() == op.offset {
                last.data.append(&mut op.data);
            } else {
                result.push(op);
            }
        }

        self.size = 0;

        result
    }
}
