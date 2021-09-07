use crate::errors::OperationResult;
use crate::modify_op::{
    CreateDirectoryOperation, CreateFileOperation, CreateSymlinkOperation, ModifyOperation,
    ModifyOperationContent, RemoveDirectoryOperation, RemoveFileOperation, RenameOperation,
    SetAttributesOperation, WriteOperation,
};
use crate::timespec::Timespec;

pub trait OperationHandler {
    fn perform_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateFileOperation,
    ) -> OperationResult<String>;

    fn perform_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateSymlinkOperation,
    ) -> OperationResult<String>;

    fn perform_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateDirectoryOperation,
    ) -> OperationResult<String>;

    fn perform_remove_file(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &RemoveFileOperation,
    ) -> OperationResult<()>;

    fn perform_remove_directory(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &RemoveDirectoryOperation,
    ) -> OperationResult<()>;

    fn perform_rename(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &RenameOperation,
    ) -> OperationResult<()>;

    fn perform_set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &SetAttributesOperation,
    ) -> OperationResult<()>;

    fn perform_write(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &WriteOperation,
    ) -> OperationResult<()>;

    fn deferred_create_file(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &CreateFileOperation,
    ) -> OperationResult<String> {
        unimplemented!()
    }

    fn deferred_create_symlink(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &CreateSymlinkOperation,
    ) -> OperationResult<String> {
        unimplemented!()
    }

    fn deferred_create_directory(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &CreateDirectoryOperation,
    ) -> OperationResult<String> {
        unimplemented!()
    }

    fn deferred_remove_file(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &RemoveFileOperation,
    ) -> OperationResult<()> {
        unimplemented!()
    }

    fn deferred_remove_directory(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &RemoveDirectoryOperation,
    ) -> OperationResult<()> {
        unimplemented!()
    }

    fn deferred_rename(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &RenameOperation,
    ) -> OperationResult<()> {
        unimplemented!()
    }

    fn deferred_set_attributes(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &SetAttributesOperation,
    ) -> OperationResult<()> {
        unimplemented!()
    }

    fn deferred_write(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &WriteOperation,
    ) -> OperationResult<()> {
        unimplemented!()
    }
}

pub struct OperationApplier;

impl OperationApplier {
    pub fn apply_operation<T: OperationHandler>(
        handler: &mut T,
        operation: &ModifyOperation,
    ) -> OperationResult<String> {
        Self::apply_operation_internal(handler, operation, false)
    }

    pub fn apply_operation_deferred<T: OperationHandler>(
        handler: &mut T,
        operation: &ModifyOperation,
    ) -> OperationResult<String> {
        Self::apply_operation_internal(handler, operation, true)
    }

    fn apply_operation_internal<T: OperationHandler>(
        handler: &mut T,
        operation: &ModifyOperation,
        deferred: bool,
    ) -> OperationResult<String> {
        let id: &str = &operation.id;
        let timestamp: Timespec = operation.timestamp.into();
        let dirent_version: i64 = operation.dirent_version;
        let content_version: i64 = operation.content_version;

        let mut new_id = id.to_owned();

        match &operation.operation {
            ModifyOperationContent::CreateFileOperation(op) => {
                new_id = Self::create_file_op(
                    handler,
                    deferred,
                    id,
                    timestamp,
                    dirent_version,
                    content_version,
                    op,
                )?
            }
            ModifyOperationContent::CreateSymlinkOperation(op) => {
                new_id = Self::create_symlink_op(
                    handler,
                    deferred,
                    id,
                    timestamp,
                    dirent_version,
                    content_version,
                    op,
                )?
            }
            ModifyOperationContent::CreateDirectoryOperation(op) => {
                new_id = Self::create_directory_op(
                    handler,
                    deferred,
                    id,
                    timestamp,
                    dirent_version,
                    content_version,
                    op,
                )?
            }
            ModifyOperationContent::RemoveFileOperation(op) => Self::remove_file_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                op,
            )?,
            ModifyOperationContent::RemoveDirectoryOperation(op) => Self::remove_directory_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                op,
            )?,
            ModifyOperationContent::RenameOperation(op) => Self::rename_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                op,
            )?,
            ModifyOperationContent::SetAttributesOperation(op) => Self::set_attributes_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                op,
            )?,
            ModifyOperationContent::WriteOperation(op) => Self::write_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                op,
            )?,
        }

        Ok(new_id)
    }

    fn create_file_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        operation: &CreateFileOperation,
    ) -> OperationResult<String> {
        if deferred {
            handler.deferred_create_file(id, timestamp, dirent_version, content_version, operation)
        } else {
            Ok(handler.perform_create_file(id, timestamp, operation)?)
        }
    }

    fn create_symlink_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        operation: &CreateSymlinkOperation,
    ) -> OperationResult<String> {
        if deferred {
            handler.deferred_create_symlink(
                id,
                timestamp,
                dirent_version,
                content_version,
                operation,
            )
        } else {
            Ok(handler.perform_create_symlink(id, timestamp, operation)?)
        }
    }

    fn create_directory_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        operation: &CreateDirectoryOperation,
    ) -> OperationResult<String> {
        if deferred {
            handler.deferred_create_directory(
                id,
                timestamp,
                dirent_version,
                content_version,
                operation,
            )
        } else {
            Ok(handler.perform_create_directory(id, timestamp, operation)?)
        }
    }

    fn remove_file_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        operation: &RemoveFileOperation,
    ) -> OperationResult<()> {
        if deferred {
            handler.deferred_remove_file(id, timestamp, dirent_version, content_version, operation)
        } else {
            Ok(handler.perform_remove_file(id, timestamp, operation)?)
        }
    }

    fn remove_directory_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        operation: &RemoveDirectoryOperation,
    ) -> OperationResult<()> {
        if deferred {
            handler.deferred_remove_directory(
                id,
                timestamp,
                dirent_version,
                content_version,
                operation,
            )
        } else {
            Ok(handler.perform_remove_directory(id, timestamp, operation)?)
        }
    }

    fn rename_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        operation: &RenameOperation,
    ) -> OperationResult<()> {
        if deferred {
            handler.deferred_rename(id, timestamp, dirent_version, content_version, operation)
        } else {
            Ok(handler.perform_rename(id, timestamp, operation)?)
        }
    }

    fn set_attributes_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        operation: &SetAttributesOperation,
    ) -> OperationResult<()> {
        if deferred {
            handler.deferred_set_attributes(
                id,
                timestamp,
                dirent_version,
                content_version,
                operation,
            )
        } else {
            Ok(handler.perform_set_attributes(id, timestamp, operation)?)
        }
    }

    fn write_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        operation: &WriteOperation,
    ) -> OperationResult<()> {
        if deferred {
            handler.deferred_write(id, timestamp, dirent_version, content_version, operation)
        } else {
            Ok(handler.perform_write(id, timestamp, operation)?)
        }
    }
}
