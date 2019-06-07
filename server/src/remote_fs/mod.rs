use std::path::Path;
use std::result;

use time::Timespec;

use offs::errors::OperationApplyError;
use offs::filesystem_capnp::modify_operation as ops;
use offs::modify_op_handler::{OperationApplier, OperationError, OperationHandler, Result};
use offs::now;
use offs::store::id_generator::{LocalTempIdGenerator, RandomHexIdGenerator};
use offs::store::wrapper::StoreWrapper;
use offs::store::{FileDev, FileMode, FileType, Store};

mod server;

macro_rules! check_content_version {
    ($id:ident, $dirent:ident, $content_version:ident) => {{
        if $dirent.content_version < $content_version {
            return Err(OperationError::InvalidOperation);
        } else if $dirent.content_version > $content_version {
            return Err(OperationError::ConflictedFile($id.to_owned()));
        }
    }};
}

pub struct RemoteFs {
    store: StoreWrapper<RandomHexIdGenerator>,
}

impl RemoteFs {
    pub fn new(mut store: Store<RandomHexIdGenerator>) -> Self {
        store.create_root_directory(0o755, now());

        Self {
            store: StoreWrapper::new(store),
        }
    }

    pub fn apply_full_journal<'a>(
        &mut self,
        op_list: impl IntoIterator<Item = ops::Reader<'a>>,
        chunks: impl IntoIterator<Item = impl IntoIterator<Item = &'a str>>,
        blobs: impl IntoIterator<Item = &'a [u8]>,
    ) -> result::Result<(Vec<String>, Vec<String>), OperationApplyError> {
        let (assigned_ids, processed_ids) = self.apply_journal(op_list)?;
        self.store.inner.add_blobs(blobs);
        for (id, file_chunks) in assigned_ids.iter().zip(chunks.into_iter()) {
            self.store
                .inner
                .replace_chunks(id, file_chunks.into_iter().enumerate());
        }

        Ok((assigned_ids, processed_ids))
    }

    pub fn apply_journal<'a>(
        &mut self,
        op_list: impl IntoIterator<Item = ops::Reader<'a>>,
    ) -> result::Result<(Vec<String>, Vec<String>), OperationApplyError> {
        let mut assigned_ids: Vec<String> = Vec::new();
        let mut processed_ids = Vec::new();
        let mut conflicted_files = Vec::new();

        for operation in op_list {
            let id = operation.get_id().unwrap();

            let result = if LocalTempIdGenerator::is_local_id(id) {
                let mut message = ::capnp::message::Builder::new_default();
                message.set_root(operation).unwrap();
                let mut new_op = message.get_root::<ops::Builder>().unwrap();

                new_op.set_id(&assigned_ids[LocalTempIdGenerator::get_n(id)]);
                OperationApplier::apply_operation_deferred(self, new_op.into_reader())
            } else {
                OperationApplier::apply_operation_deferred(self, operation)
            };

            if result.is_err() {
                match result.err().unwrap() {
                    OperationError::InvalidOperation => {
                        return Err(OperationApplyError::InvalidJournal)
                    }
                    OperationError::ConflictedFile(id) => conflicted_files.push(id),
                }

                continue;
            }

            let new_id = result.ok().unwrap();
            match operation.which().unwrap() {
                ops::CreateFile(_) | ops::CreateDirectory(_) => assigned_ids.push(new_id.clone()),
                _ => {}
            }

            processed_ids.push(new_id);
        }

        if conflicted_files.is_empty() {
            Ok((assigned_ids, processed_ids))
        } else {
            Err(OperationApplyError::ConflictingFiles(conflicted_files))
        }
    }

    fn get_conflicted_name(&mut self, parent_id: &str, name: &str, timestamp: Timespec) -> String {
        let path = Path::new(name);

        let name = path.file_stem().unwrap().to_str().unwrap();
        let ext: String = path
            .extension()
            .map_or("".to_owned(), |x| format!(".{}", x.to_str().unwrap()));

        let tm = time::at(timestamp);
        let date_str = tm.strftime("%Y-%m-%d").unwrap().to_string();

        let new_name = format!("{} (Conflicted copy {}){}", name, date_str, ext);
        if !self.store.inner.file_exists_by_name(parent_id, &new_name) {
            return new_name;
        }

        // Windows does not support colons in filenames, so we have to work around that
        let time_str = tm.strftime("%H-%M-%S").unwrap().to_string();
        let new_name = format!(
            "{} (Conflicted copy {} {}){}",
            name, date_str, time_str, ext
        );
        if !self.store.inner.file_exists_by_name(parent_id, &new_name) {
            return new_name;
        }

        for i in 2.. {
            let new_name = format!(
                "{} (Conflicted copy {} {}) ({}) {}",
                name, date_str, time_str, i, ext
            );
            if !self.store.inner.file_exists_by_name(parent_id, &new_name) {
                return new_name;
            }
        }

        // We shouldn't ever get here, as there is an infinite loop above
        unreachable!();
    }

    fn get_name_if_conflicts(
        &mut self,
        parent_id: &str,
        name: &str,
        timestamp: Timespec,
    ) -> String {
        if self.store.inner.file_exists_by_name(parent_id, name) {
            self.get_conflicted_name(parent_id, name, timestamp)
        } else {
            name.to_owned()
        }
    }

    fn get_name_if_conflicts_by_id(&mut self, id: &str, timestamp: Timespec) -> String {
        let dirent = self.store.inner.query_file(id).unwrap();

        self.get_name_if_conflicts(&dirent.parent, &dirent.name, timestamp)
    }

    fn create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> String {
        self.store.inner.increment_content_version(parent_id);

        self.store
            .create_file(parent_id, timestamp, name, file_type, mode, dev)
    }

    fn create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        link: &str,
    ) -> String {
        self.store.inner.increment_content_version(parent_id);

        self.store.create_symlink(parent_id, timestamp, name, link)
    }

    fn create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        mode: FileMode,
    ) -> String {
        self.store.inner.increment_content_version(parent_id);

        self.store
            .create_directory(parent_id, timestamp, name, mode)
    }

    fn remove_file(&mut self, id: &str, timestamp: Timespec) {
        let dirent = self.store.inner.query_file(id).unwrap();
        self.store.inner.increment_content_version(&dirent.parent);

        self.store.remove_file(id, timestamp);
    }

    fn remove_directory(&mut self, id: &str, timestamp: Timespec) {
        let dirent = self.store.inner.query_file(id).unwrap();
        self.store.inner.increment_content_version(&dirent.parent);

        self.store.remove_directory(id, timestamp);
    }

    fn rename(&mut self, id: &str, timestamp: Timespec, new_parent: &str, new_name: &str) {
        let dirent = self.store.inner.query_file(id).unwrap();
        self.store.inner.increment_content_version(&dirent.parent);
        self.store.inner.increment_content_version(&new_parent);
        self.store.inner.increment_dirent_version(id);

        self.store.rename(id, timestamp, new_parent, new_name);
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
    ) {
        if size.is_some() {
            self.store.inner.increment_content_version(id);
        } else {
            self.store.inner.increment_dirent_version(id);
        }

        self.store
            .set_attributes(id, timestamp, mode, uid, gid, size, atim, mtim);
    }

    fn write(&mut self, id: &str, timestamp: Timespec, offset: usize, data: &[u8]) {
        self.store.inner.increment_content_version(id);

        self.store.write(id, timestamp, offset, data);
    }
}

impl OperationHandler for RemoteFs {
    fn perform_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> String {
        self.create_file(parent_id, timestamp, name, file_type, mode, dev)
    }

    fn perform_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        link: &str,
    ) -> String {
        self.create_symlink(parent_id, timestamp, name, link)
    }

    fn perform_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        mode: FileMode,
    ) -> String {
        self.create_directory(parent_id, timestamp, name, mode)
    }

    fn perform_remove_file(&mut self, id: &str, timestamp: Timespec) {
        self.remove_file(id, timestamp);
    }

    fn perform_remove_directory(&mut self, id: &str, timestamp: Timespec) {
        self.remove_directory(id, timestamp);
    }

    fn perform_rename(&mut self, id: &str, timestamp: Timespec, new_parent: &str, new_name: &str) {
        self.rename(id, timestamp, new_parent, new_name);
    }

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
    ) {
        self.set_attributes(id, timestamp, mode, uid, gid, size, atim, mtim);
    }

    fn perform_write(&mut self, id: &str, timestamp: Timespec, offset: usize, data: &[u8]) {
        self.write(id, timestamp, offset, data);
    }

    fn deferred_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> Result<String> {
        let new_name = self.get_name_if_conflicts(parent_id, name, timestamp);

        Ok(self.create_file(parent_id, timestamp, &new_name, file_type, mode, dev))
    }

    fn deferred_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        name: &str,
        link: &str,
    ) -> Result<String> {
        let new_name = self.get_name_if_conflicts(parent_id, name, timestamp);

        Ok(self.create_symlink(parent_id, timestamp, &new_name, link))
    }

    fn deferred_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        name: &str,
        mode: FileMode,
    ) -> Result<String> {
        let new_name = self.get_name_if_conflicts(parent_id, name, timestamp);

        Ok(self.create_directory(parent_id, timestamp, &new_name, mode))
    }

    fn deferred_remove_file(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
    ) -> Result<()> {
        self.remove_file(id, timestamp);

        Ok(())
    }

    fn deferred_remove_directory(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
    ) -> Result<()> {
        self.remove_directory(id, timestamp);

        Ok(())
    }

    fn deferred_rename(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        _content_version: i64,
        new_parent: &str,
        _new_name: &str,
    ) -> Result<()> {
        let name = self.get_name_if_conflicts_by_id(id, timestamp);

        self.rename(id, timestamp, new_parent, &name);

        Ok(())
    }

    fn deferred_set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        content_version: i64,
        mode: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        mut size: Option<u64>,
        atim: Option<Timespec>,
        mtim: Option<Timespec>,
    ) -> Result<()> {
        if size.is_some() {
            let dirent = self.store.inner.query_file(id).unwrap();

            if dirent.stat.has_size() {
                check_content_version!(id, dirent, content_version);
            } else {
                size = None;
            }
        }

        self.set_attributes(id, timestamp, mode, uid, gid, size, atim, mtim);

        Ok(())
    }

    fn deferred_write(
        &mut self,
        id: &str,
        timestamp: Timespec,
        _dirent_version: i64,
        content_version: i64,
        offset: usize,
        data: &[u8],
    ) -> Result<()> {
        {
            let dirent = self.store.inner.query_file(id).unwrap();
            check_content_version!(id, dirent, content_version);
        }

        self.write(id, timestamp, offset, data);

        Ok(())
    }
}
