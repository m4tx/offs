use std::result;

use time::Timespec;

use crate::modify_op::{
    CreateDirectoryOperation, CreateFileOperation, CreateSymlinkOperation, ModifyOperation,
    ModifyOperationContent, RemoveDirectoryOperation, RemoveFileOperation, RenameOperation,
    SetAttributesOperation, WriteOperation,
};

pub enum OperationError {
    InvalidOperation,
    ConflictedFile(String),
}

pub type Result<T> = result::Result<T, OperationError>;

pub trait OperationHandler {
    fn perform_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateFileOperation,
    ) -> String;

    fn perform_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateSymlinkOperation,
    ) -> String;

    fn perform_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateDirectoryOperation,
    ) -> String;

    fn perform_remove_file(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &RemoveFileOperation,
    );

    fn perform_remove_directory(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &RemoveDirectoryOperation,
    );

    fn perform_rename(&mut self, id: &str, timestamp: Timespec, operation: &RenameOperation);

    fn perform_set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &SetAttributesOperation,
    );

    fn perform_write(&mut self, id: &str, timestamp: Timespec, operation: &WriteOperation);

    fn deferred_create_file(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &CreateFileOperation,
    ) -> Result<String> {
        unimplemented!()
    }

    fn deferred_create_symlink(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &CreateSymlinkOperation,
    ) -> Result<String> {
        unimplemented!()
    }

    fn deferred_create_directory(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &CreateDirectoryOperation,
    ) -> Result<String> {
        unimplemented!()
    }

    fn deferred_remove_file(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &RemoveFileOperation,
    ) -> Result<()> {
        unimplemented!()
    }

    fn deferred_remove_directory(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &RemoveDirectoryOperation,
    ) -> Result<()> {
        unimplemented!()
    }

    fn deferred_rename(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &RenameOperation,
    ) -> Result<()> {
        unimplemented!()
    }

    fn deferred_set_attributes(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &SetAttributesOperation,
    ) -> Result<()> {
        unimplemented!()
    }

    fn deferred_write(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &WriteOperation,
    ) -> Result<()> {
        unimplemented!()
    }
}

pub struct OperationApplier;

impl OperationApplier {
    pub fn apply_operation<T: OperationHandler>(
        handler: &mut T,
        operation: &ModifyOperation,
    ) -> Result<String> {
        Self::apply_operation_internal(handler, operation, false)
    }

    pub fn apply_operation_deferred<T: OperationHandler>(
        handler: &mut T,
        operation: &ModifyOperation,
    ) -> Result<String> {
        Self::apply_operation_internal(handler, operation, true)
    }

    fn apply_operation_internal<T: OperationHandler>(
        handler: &mut T,
        operation: &ModifyOperation,
        deferred: bool,
    ) -> Result<String> {
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
    ) -> Result<String> {
        if deferred {
            handler.deferred_create_file(id, timestamp, dirent_version, content_version, operation)
        } else {
            Ok(handler.perform_create_file(id, timestamp, operation))
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
    ) -> Result<String> {
        if deferred {
            handler.deferred_create_symlink(
                id,
                timestamp,
                dirent_version,
                content_version,
                operation,
            )
        } else {
            Ok(handler.perform_create_symlink(id, timestamp, operation))
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
    ) -> Result<String> {
        if deferred {
            handler.deferred_create_directory(
                id,
                timestamp,
                dirent_version,
                content_version,
                operation,
            )
        } else {
            Ok(handler.perform_create_directory(id, timestamp, operation))
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
    ) -> Result<()> {
        if deferred {
            handler.deferred_remove_file(id, timestamp, dirent_version, content_version, operation)
        } else {
            Ok(handler.perform_remove_file(id, timestamp, operation))
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
    ) -> Result<()> {
        if deferred {
            handler.deferred_remove_directory(
                id,
                timestamp,
                dirent_version,
                content_version,
                operation,
            )
        } else {
            Ok(handler.perform_remove_directory(id, timestamp, operation))
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
    ) -> Result<()> {
        if deferred {
            handler.deferred_rename(id, timestamp, dirent_version, content_version, operation)
        } else {
            Ok(handler.perform_rename(id, timestamp, operation))
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
    ) -> Result<()> {
        if deferred {
            handler.deferred_set_attributes(
                id,
                timestamp,
                dirent_version,
                content_version,
                operation,
            )
        } else {
            Ok(handler.perform_set_attributes(id, timestamp, operation))
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
    ) -> Result<()> {
        if deferred {
            handler.deferred_write(id, timestamp, dirent_version, content_version, operation)
        } else {
            Ok(handler.perform_write(id, timestamp, operation))
        }
    }
}
