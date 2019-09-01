use time::Timespec;

use offs::modify_op::{
    CreateDirectoryOperation, CreateFileOperation, CreateSymlinkOperation,
    RemoveDirectoryOperation, RemoveFileOperation, RenameOperation, SetAttributesOperation,
    WriteOperation as ModifyOpWriteOperation,
};
use offs::modify_op_handler::OperationHandler;

use super::OffsFilesystem;

impl OperationHandler for OffsFilesystem {
    fn perform_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateFileOperation,
    ) -> String {
        self.store.create_file(
            parent_id,
            timestamp,
            &operation.name,
            operation.file_type,
            operation.perm,
            operation.dev,
        )
    }

    fn perform_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateSymlinkOperation,
    ) -> String {
        self.store
            .create_symlink(parent_id, timestamp, &operation.name, &operation.link)
    }

    fn perform_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateDirectoryOperation,
    ) -> String {
        self.store
            .create_directory(parent_id, timestamp, &operation.name, operation.perm)
    }

    fn perform_remove_file(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _operation: &RemoveFileOperation,
    ) {
        self.store.remove_file(id, timestamp);
    }

    fn perform_remove_directory(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _operation: &RemoveDirectoryOperation,
    ) {
        self.store.remove_directory(id, timestamp);
    }

    fn perform_rename(&mut self, id: &str, timestamp: Timespec, operation: &RenameOperation) {
        self.store
            .rename(id, timestamp, &operation.new_parent, &operation.new_name);
    }

    fn perform_set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &SetAttributesOperation,
    ) {
        self.store.set_attributes(
            id,
            timestamp,
            operation.perm,
            operation.uid,
            operation.gid,
            operation.size,
            operation.atim,
            operation.mtim,
        );
    }

    fn perform_write(&mut self, id: &str, timestamp: Timespec, operation: &ModifyOpWriteOperation) {
        self.store
            .write(id, timestamp, operation.offset as usize, &operation.data);
    }
}
