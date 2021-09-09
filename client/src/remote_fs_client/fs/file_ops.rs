use std::sync::atomic::Ordering;

use prost::Message;

use offs::modify_op::ModifyOperation;
use offs::modify_op_handler::OperationApplier;
use offs::proto::filesystem as proto_types;
use offs::store::{DirEntity, FileDev, FileMode, FileType};

use super::super::client::modify_op_builder::ModifyOpBuilder;
use super::write_buffer::WriteOperation;
use super::OffsFilesystem;
use offs::errors::{OperationError, OperationResult};
use offs::timespec::Timespec;

impl OffsFilesystem {
    // File operations
    pub(super) async fn close_all_files(&mut self) -> OperationResult<()> {
        for fh in self.open_file_handler.get_file_handles() {
            self.flush_write_buffer(fh).await?;
        }

        Ok(())
    }

    pub(super) async fn flush_write_buffer(&mut self, fh: u64) -> OperationResult<()> {
        let (id, operations) = self.open_file_handler.flush(fh);
        for op in operations.into_iter() {
            self.do_single_write(&id, op).await?;
        }

        Ok(())
    }

    async fn do_single_write(&mut self, id: &str, op: WriteOperation) -> OperationResult<()> {
        let dirent = self.store.query_file(id)?;
        let operation = ModifyOpBuilder::make_write_op(&dirent, op.offset as i64, op.data);

        let mut dirent = self.perform_operation(operation).await?;
        self.add_dirent(&mut dirent)?;

        Ok(())
    }

    // Read
    pub(super) async fn list_files(&mut self, id: &str) -> OperationResult<Vec<DirEntity>> {
        if self.is_offline() {
            let dirent = self.store.query_file(id)?;
            if !dirent.is_retrieved() {
                err_offline!();
            }

            return Ok(self.store.list_files(id)?);
        }

        let mut items = self.client.list_files(id).await?;

        let transaction = self.store.transaction();
        for dirent in &mut items {
            self.add_dirent(dirent)?;
        }
        self.store.update_retrieved_version(id)?;

        let children_ids = items.iter().map(|x| &x.id);
        self.store.remove_remaining_files(id, children_ids)?;

        transaction.commit()?;

        Ok(items)
    }

    pub(super) async fn read(
        &mut self,
        id: &str,
        offset: i64,
        size: u32,
    ) -> OperationResult<Vec<u8>> {
        let missing_blobs = self.store.get_missing_blobs_for_read(id, offset, size)?;
        self.retrieve_missing_blobs(missing_blobs).await?;

        Ok(self.store.read(id, offset, size)?)
    }

    // Modifications

    fn apply_operation(&mut self, operation: &ModifyOperation) -> OperationResult<String> {
        Ok(OperationApplier::apply_operation(self, operation)?)
    }

    async fn perform_operation(
        &mut self,
        operation: ModifyOperation,
    ) -> OperationResult<DirEntity> {
        if self.should_flush_journal.load(Ordering::Relaxed) {
            self.apply_journal().await?;
        }

        let id = operation.id.clone();
        let serialized_op = proto_types::ModifyOperation::from(operation.clone()).encode_to_vec();

        let transaction = self.store.transaction();

        let new_id = self.apply_operation(&operation)?;

        let journal_entry_id = self.store.add_journal_entry(&id, &serialized_op)?;
        let dirent = if self.is_offline() {
            self.store.query_file(&new_id)?
        } else {
            let mut dirent = self.client.request_apply_operation(operation).await?;
            self.store.remove_journal_item(journal_entry_id)?;

            if !dirent.id.is_empty() {
                if new_id != dirent.id {
                    self.store.change_id(&new_id, &dirent.id)?;
                }
                self.add_dirent(&mut dirent)?;
            }

            dirent
        };

        transaction.commit()?;

        Ok(dirent)
    }

    // Create
    pub(super) async fn create_file(
        &mut self,
        parent_id: &str,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> OperationResult<DirEntity> {
        let parent_dirent = self.store.query_file(parent_id)?;
        let operation =
            ModifyOpBuilder::make_create_file_op(&parent_dirent, name, file_type, mode, dev);

        let mut dirent = self.perform_operation(operation).await?;
        self.add_dirent(&mut dirent)?;

        Ok(dirent)
    }

    pub(super) async fn create_symlink(
        &mut self,
        parent_id: &str,
        name: &str,
        link: &str,
    ) -> OperationResult<DirEntity> {
        let parent_dirent = self.store.query_file(parent_id)?;
        let operation = ModifyOpBuilder::make_create_symlink_op(&parent_dirent, name, link);

        let mut dirent = self.perform_operation(operation).await?;
        self.add_dirent(&mut dirent)?;

        Ok(dirent)
    }

    pub(super) async fn create_directory(
        &mut self,
        parent_id: &str,
        name: &str,
        mode: FileMode,
    ) -> OperationResult<DirEntity> {
        let parent_dirent = self.store.query_file(parent_id)?;
        let operation = ModifyOpBuilder::make_create_directory_op(&parent_dirent, name, mode);

        let mut dirent = self.perform_operation(operation).await?;
        self.add_dirent(&mut dirent)?;

        Ok(dirent)
    }

    // Remove
    pub(super) async fn remove_file(&mut self, id: &str) -> OperationResult<()> {
        let dirent = self.store.query_file(id)?;
        let operation = ModifyOpBuilder::make_remove_file_op(&dirent);

        self.perform_operation(operation).await?;

        Ok(())
    }

    pub(super) async fn remove_directory(&mut self, id: &str) -> OperationResult<()> {
        let dirent = self.store.query_file(id)?;
        let operation = ModifyOpBuilder::make_remove_directory_op(&dirent);

        self.perform_operation(operation).await?;

        Ok(())
    }

    // Modify
    pub(super) async fn rename_file(
        &mut self,
        id: &str,
        new_parent: &str,
        new_name: &str,
    ) -> OperationResult<DirEntity> {
        let dirent = self.store.query_file(id)?;
        let operation = ModifyOpBuilder::make_rename_op(&dirent, new_parent, new_name);

        let mut dirent = self.perform_operation(operation).await?;
        self.add_dirent(&mut dirent)?;

        Ok(dirent)
    }

    pub(super) async fn set_attributes(
        &mut self,
        id: &str,
        mode: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
    ) -> OperationResult<DirEntity> {
        let dirent = self.store.query_file(id)?;
        let operation =
            ModifyOpBuilder::make_set_attributes_op(&dirent, mode, uid, gid, size, atime, mtime);

        let mut dirent = self.perform_operation(operation).await?;
        self.add_dirent(&mut dirent)?;

        Ok(dirent)
    }

    pub(super) async fn write(
        &mut self,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
    ) -> OperationResult<()> {
        let should_flush = self
            .open_file_handler
            .write(fh, WriteOperation::new(offset as usize, data));

        if should_flush {
            self.flush_write_buffer(fh).await?;
        }

        Ok(())
    }
}
