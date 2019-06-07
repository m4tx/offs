use std::result;

use time::Timespec;

use crate::filesystem_capnp::modify_operation as ops;
use crate::store::{FileDev, FileMode, FileType};

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
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> String;

    fn perform_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        link: &str,
    ) -> String;

    fn perform_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        mode: FileMode,
    ) -> String;

    fn perform_remove_file(&mut self, id: &str, timestamp: Timespec);

    fn perform_remove_directory(&mut self, id: &str, timestamp: Timespec);

    fn perform_rename(&mut self, id: &str, timestamp: Timespec, new_parent: &str, new_name: &str);

    fn perform_set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        mode: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atim: Option<Timespec>,
        mtim: Option<Timespec>,
    );

    fn perform_write(&mut self, id: &str, timestamp: Timespec, offset: usize, data: &[u8]);

    fn deferred_create_file(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _name: &str,
        _file_type: FileType,
        _mode: FileMode,
        _dev: FileDev,
    ) -> Result<String> {
        unimplemented!()
    }

    fn deferred_create_symlink(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _name: &str,
        _link: &str,
    ) -> Result<String> {
        unimplemented!()
    }

    fn deferred_create_directory(
        &mut self,
        _parent_id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _name: &str,
        _mode: FileMode,
    ) -> Result<String> {
        unimplemented!()
    }

    fn deferred_remove_file(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
    ) -> Result<()> {
        unimplemented!()
    }

    fn deferred_remove_directory(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
    ) -> Result<()> {
        unimplemented!()
    }

    fn deferred_rename(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _new_parent: &str,
        _new_name: &str,
    ) -> Result<()> {
        unimplemented!()
    }

    fn deferred_set_attributes(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _mode: Option<FileMode>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atim: Option<Timespec>,
        _mtim: Option<Timespec>,
    ) -> Result<()> {
        unimplemented!()
    }

    fn deferred_write(
        &mut self,
        _id: &str,
        _timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _offset: usize,
        _data: &[u8],
    ) -> Result<()> {
        unimplemented!()
    }
}

pub struct OperationApplier;

impl OperationApplier {
    const DEFAULT_MODE: FileMode = std::u16::MAX;
    const DEFAULT_UID: u32 = std::u32::MAX;
    const DEFAULT_GID: u32 = std::u32::MAX;
    const DEFAULT_SIZE: u64 = std::u64::MAX;

    fn check_default_value<T: PartialEq>(value: T, default: T) -> Option<T> {
        if value == default {
            None
        } else {
            Some(value)
        }
    }

    pub fn apply_operation<T: OperationHandler>(
        handler: &mut T,
        operation: ops::Reader,
    ) -> Result<String> {
        Self::apply_operation_internal(handler, operation, false)
    }

    pub fn apply_operation_deferred<T: OperationHandler>(
        handler: &mut T,
        operation: ops::Reader,
    ) -> Result<String> {
        Self::apply_operation_internal(handler, operation, true)
    }

    fn apply_operation_internal<T: OperationHandler>(
        handler: &mut T,
        operation: ops::Reader,
        deferred: bool,
    ) -> Result<String> {
        let id: &str = operation.get_id().unwrap();
        let timestamp: Timespec = operation.get_timestamp().unwrap().into();
        let dirent_version: i64 = operation.get_dirent_version();
        let content_version: i64 = operation.get_content_version();

        let mut new_id = id.to_owned();

        match operation.which().unwrap() {
            ops::CreateFile(params) => {
                new_id = Self::create_file_op(
                    handler,
                    deferred,
                    id,
                    timestamp,
                    dirent_version,
                    content_version,
                    params,
                )?
            }
            ops::CreateSymlink(params) => {
                new_id = Self::create_symlink_op(
                    handler,
                    deferred,
                    id,
                    timestamp,
                    dirent_version,
                    content_version,
                    params,
                )?
            }
            ops::CreateDirectory(params) => {
                new_id = Self::create_directory_op(
                    handler,
                    deferred,
                    id,
                    timestamp,
                    dirent_version,
                    content_version,
                    params,
                )?
            }
            ops::RemoveFile(params) => Self::remove_file_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                params,
            )?,
            ops::RemoveDirectory(params) => Self::remove_directory_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                params,
            )?,
            ops::Rename(params) => Self::rename_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                params,
            )?,
            ops::SetAttributes(params) => Self::set_attributes_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                params,
            )?,
            ops::Write(params) => Self::write_op(
                handler,
                deferred,
                id,
                timestamp,
                dirent_version,
                content_version,
                params,
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
        params: ops::create_file::Reader,
    ) -> Result<String> {
        let name = params.get_name().unwrap();
        let file_type = params.get_file_type().unwrap().into();
        let mode = params.get_mode();
        let dev = params.get_dev();

        if deferred {
            handler.deferred_create_file(
                id,
                timestamp,
                dirent_version,
                content_version,
                name,
                file_type,
                mode,
                dev,
            )
        } else {
            Ok(handler.perform_create_file(id, timestamp, name, file_type, mode, dev))
        }
    }

    fn create_symlink_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        params: ops::create_symlink::Reader,
    ) -> Result<String> {
        let name = params.get_name().unwrap();
        let link = params.get_link().unwrap();

        if deferred {
            handler.deferred_create_symlink(
                id,
                timestamp,
                dirent_version,
                content_version,
                name,
                link,
            )
        } else {
            Ok(handler.perform_create_symlink(id, timestamp, name, link))
        }
    }

    fn create_directory_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        params: ops::create_directory::Reader,
    ) -> Result<String> {
        let name = params.get_name().unwrap();
        let mode = params.get_mode();

        if deferred {
            handler.deferred_create_directory(
                id,
                timestamp,
                dirent_version,
                content_version,
                name,
                mode,
            )
        } else {
            Ok(handler.perform_create_directory(id, timestamp, name, mode))
        }
    }

    fn remove_file_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        _params: ops::remove_file::Reader,
    ) -> Result<()> {
        if deferred {
            handler.deferred_remove_file(id, timestamp, dirent_version, content_version)
        } else {
            Ok(handler.perform_remove_file(id, timestamp))
        }
    }

    fn remove_directory_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        _params: ops::remove_directory::Reader,
    ) -> Result<()> {
        if deferred {
            handler.deferred_remove_directory(id, timestamp, dirent_version, content_version)
        } else {
            Ok(handler.perform_remove_directory(id, timestamp))
        }
    }

    fn rename_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        params: ops::rename::Reader,
    ) -> Result<()> {
        let new_parent = params.get_new_parent().unwrap();
        let new_name = params.get_new_name().unwrap();

        if deferred {
            handler.deferred_rename(
                id,
                timestamp,
                dirent_version,
                content_version,
                new_parent,
                new_name,
            )
        } else {
            Ok(handler.perform_rename(id, timestamp, new_parent, new_name))
        }
    }

    fn set_attributes_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        params: ops::set_attributes::Reader,
    ) -> Result<()> {
        let to_save = params.get_to_save().unwrap();
        let mode: FileMode = to_save.get_mode();
        let uid: u32 = to_save.get_uid();
        let gid: u32 = to_save.get_gid();
        let size: u64 = to_save.get_size();
        let atim: Option<Timespec> = if to_save.has_atim() {
            Some(to_save.get_atim().unwrap().into())
        } else {
            None
        };
        let mtim: Option<Timespec> = if to_save.has_mtim() {
            Some(to_save.get_mtim().unwrap().into())
        } else {
            None
        };

        let mode = Self::check_default_value(mode, Self::DEFAULT_MODE);
        let uid = Self::check_default_value(uid, Self::DEFAULT_UID);
        let gid = Self::check_default_value(gid, Self::DEFAULT_GID);
        let size = Self::check_default_value(size, Self::DEFAULT_SIZE);

        if deferred {
            handler.deferred_set_attributes(
                id,
                timestamp,
                dirent_version,
                content_version,
                mode,
                uid,
                gid,
                size,
                atim,
                mtim,
            )
        } else {
            Ok(handler.perform_set_attributes(id, timestamp, mode, uid, gid, size, atim, mtim))
        }
    }

    fn write_op<T: OperationHandler>(
        handler: &mut T,
        deferred: bool,
        id: &str,
        timestamp: Timespec,
        dirent_version: i64,
        content_version: i64,
        params: ops::write::Reader,
    ) -> Result<()> {
        let offset = params.get_offset();
        let data = params.get_data().unwrap();

        if deferred {
            handler.deferred_write(
                id,
                timestamp,
                dirent_version,
                content_version,
                offset as usize,
                data,
            )
        } else {
            Ok(handler.perform_write(id, timestamp, offset as usize, data))
        }
    }
}
