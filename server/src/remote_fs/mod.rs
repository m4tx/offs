use std::path::Path;

use offs::errors::{
    JournalApplyData, JournalApplyError, JournalApplyResult, OperationError, OperationErrorType,
    OperationResult,
};

use offs::modify_op::{
    CreateDirectoryOperation, CreateFileOperation, CreateSymlinkOperation, ModifyOperation,
    ModifyOperationContent, RemoveDirectoryOperation, RemoveFileOperation, RenameOperation,
    SetAttributesOperation, WriteOperation,
};
use offs::modify_op_handler::{OperationApplier, OperationHandler};
use offs::now;
use offs::store::id_generator::{LocalTempIdGenerator, RandomHexIdGenerator};
use offs::store::wrapper::StoreWrapper;
use offs::store::{FileDev, FileMode, FileType, Store};

mod grpc_server;
use chrono::{TimeZone, Utc};
pub use grpc_server::RemoteFsServerImpl;
use offs::timespec::Timespec;

macro_rules! check_content_version {
    ($id:ident, $dirent:ident, $content_version:ident) => {{
        if $dirent.content_version < $content_version {
            return Err(OperationError::invalid_content_version());
        } else if $dirent.content_version > $content_version {
            return Err(OperationError::conflicted_file($id.to_owned()));
        }
    }};
}

#[derive(Clone)]
pub struct RemoteFs {
    store: StoreWrapper<RandomHexIdGenerator>,
}

impl RemoteFs {
    pub fn new(mut store: Store<RandomHexIdGenerator>) -> OperationResult<Self> {
        store.create_root_directory(0o755, now())?;

        Ok(Self {
            store: StoreWrapper::new(store),
        })
    }

    pub fn apply_full_journal(
        &mut self,
        op_list: impl IntoIterator<Item = ModifyOperation>,
        chunks: impl IntoIterator<Item = impl IntoIterator<Item = impl AsRef<str>>>,
        blobs: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> JournalApplyResult {
        let (assigned_ids, processed_ids) = self.apply_journal(op_list)?;
        let dir_entities = processed_ids
            .iter()
            .filter_map(|id| self.store.try_query_file(id).unwrap())
            .collect();

        self.store.add_blobs(blobs).unwrap();
        for (id, file_chunks) in assigned_ids.iter().zip(chunks.into_iter()) {
            self.store
                .replace_chunks(id, file_chunks.into_iter().enumerate())
                .unwrap();
        }

        Ok(JournalApplyData {
            assigned_ids,
            dir_entities,
        })
    }

    pub fn apply_journal(
        &mut self,
        op_list: impl IntoIterator<Item = ModifyOperation>,
    ) -> Result<(Vec<String>, Vec<String>), JournalApplyError> {
        let mut assigned_ids: Vec<String> = Vec::new();
        let mut processed_ids = Vec::new();
        let mut conflicted_files = Vec::new();

        for mut operation in op_list {
            let id = &operation.id;

            let add_assigned_id = match operation.operation {
                ModifyOperationContent::CreateFileOperation(_)
                | ModifyOperationContent::CreateSymlinkOperation(_)
                | ModifyOperationContent::CreateDirectoryOperation(_) => true,
                _ => false,
            };

            let result = if LocalTempIdGenerator::is_local_id(id) {
                operation.id = assigned_ids[LocalTempIdGenerator::get_n(id)].clone();
                OperationApplier::apply_operation_deferred(self, &operation)
            } else {
                OperationApplier::apply_operation_deferred(self, &operation)
            };

            if result.is_err() {
                let err = result.err().unwrap();
                match err.error_type {
                    OperationErrorType::ConflictedFile => {
                        conflicted_files.push(String::from_utf8_lossy(&err.details).to_string())
                    }
                    _ => return Err(JournalApplyError::InvalidJournal),
                };

                continue;
            }

            let new_id = result.ok().unwrap();
            if add_assigned_id {
                assigned_ids.push(new_id.clone());
            }

            processed_ids.push(new_id);
        }

        if conflicted_files.is_empty() {
            Ok((assigned_ids, processed_ids))
        } else {
            Err(JournalApplyError::ConflictingFiles(conflicted_files))
        }
    }

    fn get_name_if_conflicts_by_id(
        &self,
        id: &str,
        timestamp: Timespec,
    ) -> OperationResult<String> {
        let dirent = self.store.query_file(id)?;

        Ok(self.get_name_if_conflicts(&dirent.parent, &dirent.name, timestamp)?)
    }

    fn get_name_if_conflicts(
        &self,
        parent_id: &str,
        name: &str,
        timestamp: Timespec,
    ) -> OperationResult<String> {
        let result = if self.store.file_exists_by_name(parent_id, name)? {
            self.get_conflicted_name(parent_id, name, timestamp)?
        } else {
            name.to_owned()
        };

        Ok(result)
    }

    fn get_conflicted_name(
        &self,
        parent_id: &str,
        name: &str,
        timestamp: Timespec,
    ) -> OperationResult<String> {
        let path = Path::new(name);

        let name = path.file_stem().unwrap().to_str().unwrap();
        let ext: String = path
            .extension()
            .map_or("".to_owned(), |x| format!(".{}", x.to_str().unwrap()));

        let datetime = Utc.timestamp(timestamp.sec, timestamp.nsec);
        let date_str = datetime.format("%Y-%m-%d").to_string();

        let new_name = format!("{} (Conflicted copy {}){}", name, date_str, ext);
        if !self.store.file_exists_by_name(parent_id, &new_name)? {
            return Ok(new_name);
        }

        // Windows does not support colons in filenames, so we have to work around that
        let time_str = datetime.format("%H-%M-%S").to_string();
        let new_name = format!(
            "{} (Conflicted copy {} {}){}",
            name, date_str, time_str, ext
        );
        if !self.store.file_exists_by_name(parent_id, &new_name)? {
            return Ok(new_name);
        }

        for i in 2.. {
            let new_name = format!(
                "{} (Conflicted copy {} {}) ({}) {}",
                name, date_str, time_str, i, ext
            );
            if !self.store.file_exists_by_name(parent_id, &new_name)? {
                return Ok(new_name);
            }
        }

        // We shouldn't ever get here, as there is an infinite loop above
        unreachable!();
    }

    fn create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> OperationResult<String> {
        self.store.increment_content_version(parent_id)?;

        Ok(self
            .store
            .create_file(parent_id, timestamp, name, file_type, mode, dev)?)
    }

    fn create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        link: &str,
    ) -> OperationResult<String> {
        self.store.increment_content_version(parent_id)?;

        Ok(self
            .store
            .create_symlink(parent_id, timestamp, name, link)?)
    }

    fn create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        mode: FileMode,
    ) -> OperationResult<String> {
        self.store.increment_content_version(parent_id)?;

        Ok(self
            .store
            .create_directory(parent_id, timestamp, name, mode)?)
    }

    fn remove_file(&mut self, id: &str, timestamp: Timespec) -> OperationResult<()> {
        let dirent = self.store.query_file(id)?;
        self.store.increment_content_version(&dirent.parent)?;

        self.store.remove_file(id, timestamp)?;

        Ok(())
    }

    fn remove_directory(&mut self, id: &str, timestamp: Timespec) -> OperationResult<()> {
        let dirent = self.store.query_file(id)?;
        self.store.increment_content_version(&dirent.parent)?;

        if self.store.any_child_exists(id)? {
            return Err(OperationError::directory_not_empty());
        }
        self.store.remove_directory(id, timestamp)?;

        Ok(())
    }

    fn rename(
        &mut self,
        id: &str,
        timestamp: Timespec,
        new_parent: &str,
        new_name: &str,
    ) -> OperationResult<()> {
        let dirent = self.store.query_file(id)?;
        self.store.increment_content_version(&dirent.parent)?;
        self.store.increment_content_version(&new_parent)?;
        self.store.increment_dirent_version(id)?;

        self.store.rename(id, timestamp, new_parent, new_name)?;

        Ok(())
    }

    fn set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        mode: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atim: Option<Timespec>,
        mtim: Option<Timespec>,
    ) -> OperationResult<()> {
        if size.is_some() {
            self.store.increment_content_version(id)?;
        } else {
            self.store.increment_dirent_version(id)?;
        }

        self.store
            .set_attributes(id, timestamp, mode, uid, gid, size, atim, mtim)?;

        Ok(())
    }

    fn write(
        &mut self,
        id: &str,
        timestamp: Timespec,
        offset: usize,
        data: &[u8],
    ) -> OperationResult<()> {
        self.store.increment_content_version(id)?;

        self.store.write(id, timestamp, offset, data)?;

        Ok(())
    }
}

impl OperationHandler for RemoteFs {
    fn perform_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateFileOperation,
    ) -> OperationResult<String> {
        Ok(self.create_file(
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
        Ok(self.create_symlink(parent_id, timestamp, &operation.name, &operation.link)?)
    }

    fn perform_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        operation: &CreateDirectoryOperation,
    ) -> OperationResult<String> {
        Ok(self.create_directory(parent_id, timestamp, &operation.name, operation.perm)?)
    }

    fn perform_remove_file(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _operation: &RemoveFileOperation,
    ) -> OperationResult<()> {
        self.remove_file(id, timestamp)?;
        Ok(())
    }

    fn perform_remove_directory(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _operation: &RemoveDirectoryOperation,
    ) -> OperationResult<()> {
        self.remove_directory(id, timestamp)?;
        Ok(())
    }

    fn perform_rename(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &RenameOperation,
    ) -> OperationResult<()> {
        self.rename(id, timestamp, &operation.new_parent, &operation.new_name)?;
        Ok(())
    }

    fn perform_set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        operation: &SetAttributesOperation,
    ) -> OperationResult<()> {
        self.set_attributes(
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
        operation: &WriteOperation,
    ) -> OperationResult<()> {
        self.write(id, timestamp, operation.offset as usize, &operation.data)?;

        Ok(())
    }

    fn deferred_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        operation: &CreateFileOperation,
    ) -> OperationResult<String> {
        let new_name = self.get_name_if_conflicts(parent_id, &operation.name, timestamp)?;

        Ok(self.create_file(
            parent_id,
            timestamp,
            &new_name,
            operation.file_type,
            operation.perm,
            operation.dev,
        )?)
    }

    fn deferred_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        operation: &CreateSymlinkOperation,
    ) -> OperationResult<String> {
        let new_name = self.get_name_if_conflicts(parent_id, &operation.name, timestamp)?;

        Ok(self.create_symlink(parent_id, timestamp, &new_name, &operation.link)?)
    }

    fn deferred_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        operation: &CreateDirectoryOperation,
    ) -> OperationResult<String> {
        let new_name = self.get_name_if_conflicts(parent_id, &operation.name, timestamp)?;

        Ok(self.create_directory(parent_id, timestamp, &new_name, operation.perm)?)
    }

    fn deferred_remove_file(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &RemoveFileOperation,
    ) -> OperationResult<()> {
        self.remove_file(id, timestamp)?;

        Ok(())
    }

    fn deferred_remove_directory(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        _operation: &RemoveDirectoryOperation,
    ) -> OperationResult<()> {
        self.remove_directory(id, timestamp)?;

        Ok(())
    }

    fn deferred_rename(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        operation: &RenameOperation,
    ) -> OperationResult<()> {
        let name = self.get_name_if_conflicts_by_id(id, timestamp)?;

        self.rename(id, timestamp, &operation.new_parent, &name)?;

        Ok(())
    }

    fn deferred_set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        content_version: i64,
        operation: &SetAttributesOperation,
    ) -> OperationResult<()> {
        let mut size = operation.size;

        if size.is_some() {
            let dirent = self.store.query_file(id)?;

            if dirent.stat.has_size() {
                check_content_version!(id, dirent, content_version);
            } else {
                size = None;
            }
        }

        self.set_attributes(
            id,
            timestamp,
            operation.perm,
            operation.uid,
            operation.gid,
            size,
            operation.atim,
            operation.mtim,
        )?;

        Ok(())
    }

    fn deferred_write(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        content_version: i64,
        operation: &WriteOperation,
    ) -> OperationResult<()> {
        {
            let dirent = self.store.query_file(id)?;
            check_content_version!(id, dirent, content_version);
        }

        self.write(id, timestamp, operation.offset as usize, &operation.data)?;

        Ok(())
    }
}
