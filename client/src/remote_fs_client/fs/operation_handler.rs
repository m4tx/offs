use offs::modify_op::{
    CreateDirectoryOperation, CreateFileOperation, CreateSymlinkOperation,
    RemoveDirectoryOperation, RemoveFileOperation, RenameOperation, SetAttributesOperation,
    WriteOperation as ModifyOpWriteOperation,
};
use offs::modify_op_handler::OperationHandler;

use super::OffsFilesystem;
use offs::errors::OperationResult;
use offs::timespec::Timespec;

impl OperationHandler for OffsFilesystem {
    fn perform_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateFileOperation,
    ) -> OperationResult<String> {
        Ok(self.store.create_file(
            parent_id,
            timestamp,
            &operation.name,
            operation.file_type,
            operation.perm,
            operation.dev,
        )?)
    }

    fn perform_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateSymlinkOperation,
    ) -> OperationResult<String> {
        Ok(self
            .store
            .create_symlink(parent_id, timestamp, &operation.name, &operation.link)?)
    }

    fn perform_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateDirectoryOperation,
    ) -> OperationResult<String> {
        Ok(self
            .store
            .create_directory(parent_id, timestamp, &operation.name, operation.perm)?)
    }

    fn perform_remove_file(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _operation: &RemoveFileOperation,
    ) -> OperationResult<()> {
        self.store.remove_file(id, timestamp)?;
        Ok(())
    }

    fn perform_remove_directory(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _operation: &RemoveDirectoryOperation,
    ) -> OperationResult<()> {
        self.store.remove_directory(id, timestamp)?;
        Ok(())
    }

    fn perform_rename(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &RenameOperation,
    ) -> OperationResult<()> {
        self.store
            .rename(id, timestamp, &operation.new_parent, &operation.new_name)?;
        Ok(())
    }

    fn perform_set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &SetAttributesOperation,
    ) -> OperationResult<()> {
        self.store.set_attributes(
            id,
            timestamp,
            operation.perm,
            operation.uid,
            operation.gid,
            operation.size,
            operation.atim,
            operation.mtim,
        )?;
        Ok(())
    }

    fn perform_write(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &ModifyOpWriteOperation,
    ) -> OperationResult<()> {
        self.store
            .write(id, timestamp, operation.offset as usize, &operation.data)?;
        Ok(())
    }
}
