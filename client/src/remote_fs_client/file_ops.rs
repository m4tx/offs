use std::sync::atomic::Ordering;

use protobuf::Message;
use time::Timespec;

use offs::modify_op::ModifyOperation;
use offs::modify_op_handler::OperationApplier;
use offs::proto::filesystem as proto_types;
use offs::store::{DirEntity, FileDev, FileMode, FileType};

use crate::remote_fs_client::client::modify_op_builder::ModifyOpBuilder;
use crate::remote_fs_client::error::{RemoteFsError, RemoteFsErrorKind};

use super::{OffsFilesystem, Result};
use super::write_buffer::WriteOperation;

impl OffsFilesystem {
    // File operations
    pub(super) fn flush_write_buffer(&mut self) -> Result<()> {
        for op in self.write_buffer.flush().into_iter() {
            self.do_single_write(op)?;
        }

        Ok(())
    }

    fn do_single_write(&mut self, op: WriteOperation) -> Result<()> {
        let dirent = self.query_file(&op.id)?;
        let operation = ModifyOpBuilder::make_write_op(&dirent, op.offset as i64, op.data);

        let mut dirent = self.perform_operation(operation)?;
        self.add_dirent(&mut dirent);

        Ok(())
    }

    // Read
    pub(super) fn list_files(&mut self, id: &str) -> Result<Vec<DirEntity>> {
        if self.is_offline() {
            let dirent = self.query_file(id)?;
            if !dirent.is_retrieved() {
                err_offline!();
            }

            return Ok(self.store.inner.list_files(id));
        }

        let mut items = self.client.list_files(id);

        let transaction = self.store.inner.transaction();
        for dirent in &mut items {
            self.add_dirent(dirent);
        }
        self.store.inner.update_retrieved_version(id);

        let children_ids = items.iter().map(|x| &x.id);
        self.store.inner.remove_remaining_files(id, children_ids);

        transaction.commit()?;

        Ok(items)
    }

    pub(super) fn read(&mut self, id: &str, offset: i64, size: u32) -> Result<Vec<u8>> {
        let missing_blobs = self.store.get_missing_blobs_for_read(id, offset, size);
        self.retrieve_missing_blobs(missing_blobs)?;

        Ok(self.store.read(id, offset, size))
    }

    // Modifications

    fn apply_operation(&mut self, operation: &ModifyOperation) -> String {
        OperationApplier::apply_operation(self, operation)
            .ok()
            .unwrap()
    }

    fn perform_operation(&mut self, operation: ModifyOperation) -> Result<DirEntity> {
        if self.should_flush_journal.load(Ordering::Relaxed) {
            self.apply_journal();
        }

        let id = operation.id.clone();

        let op_proto: proto_types::ModifyOperation = operation.into();
        let serialized_op = op_proto.write_to_bytes().unwrap();
        let operation: ModifyOperation = op_proto.into();

        let transaction = self.store.inner.transaction();

        let new_id = self.apply_operation(&operation);

        let journal_entry_id = self.store.inner.add_journal_entry(&id, &serialized_op);
        let dirent = if self.is_offline() {
            self.query_file(&new_id)?
        } else {
            let mut dirent = self.client.request_apply_operation(operation);
            self.store.inner.remove_journal_item(journal_entry_id);

            if !dirent.id.is_empty() {
                if new_id != dirent.id {
                    self.store.inner.change_id(&new_id, &dirent.id);
                }
                self.add_dirent(&mut dirent);
            }

            dirent
        };

        transaction.commit()?;

        Ok(dirent)
    }

    // Create
    pub(super) fn create_file(
        &mut self,
        parent_id: &str,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> Result<DirEntity> {
        let parent_dirent = self.query_file(parent_id)?;
        let operation =
            ModifyOpBuilder::make_create_file_op(&parent_dirent, name, file_type, mode, dev);

        let mut dirent = self.perform_operation(operation)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    pub(super) fn create_symlink(
        &mut self,
        parent_id: &str,
        name: &str,
        link: &str,
    ) -> Result<DirEntity> {
        let parent_dirent = self.query_file(parent_id)?;
        let operation = ModifyOpBuilder::make_create_symlink_op(&parent_dirent, name, link);

        let mut dirent = self.perform_operation(operation)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    pub(super) fn create_directory(
        &mut self,
        parent_id: &str,
        name: &str,
        mode: FileMode,
    ) -> Result<DirEntity> {
        let parent_dirent = self.query_file(parent_id)?;
        let operation = ModifyOpBuilder::make_create_directory_op(&parent_dirent, name, mode);

        let mut dirent = self.perform_operation(operation)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    // Remove
    pub(super) fn remove_file(&mut self, id: &str) -> Result<()> {
        let dirent = self.query_file(id)?;
        let operation = ModifyOpBuilder::make_remove_file_op(&dirent);

        self.perform_operation(operation)?;

        Ok(())
    }

    pub(super) fn remove_directory(&mut self, id: &str) -> Result<()> {
        let dirent = self.query_file(id)?;
        let operation = ModifyOpBuilder::make_remove_directory_op(&dirent);

        self.perform_operation(operation)?;

        Ok(())
    }

    // Modify
    pub(super) fn rename_file(
        &mut self,
        id: &str,
        new_parent: &str,
        new_name: &str,
    ) -> Result<DirEntity> {
        let dirent = self.query_file(id)?;
        let operation = ModifyOpBuilder::make_rename_op(&dirent, new_parent, new_name);

        let mut dirent = self.perform_operation(operation)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    pub(super) fn set_attributes(
        &mut self,
        id: &str,
        mode: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
    ) -> Result<DirEntity> {
        let dirent = self.query_file(id)?;
        let operation =
            ModifyOpBuilder::make_set_attributes_op(&dirent, mode, uid, gid, size, atime, mtime);

        let mut dirent = self.perform_operation(operation)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    pub(super) fn write(&mut self, id: &str, offset: i64, data: &[u8]) -> Result<()> {
        self.write_buffer.add_write_op(WriteOperation::new(
            id.to_owned(),
            offset as usize,
            data.to_owned(),
        ));

        if self.write_buffer.is_full() {
            self.flush_write_buffer()?;
        }

        Ok(())
    }
}
