use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use protobuf::Message;
use time::Timespec;

use offs::modify_op::ModifyOperation;
use offs::modify_op_handler::OperationApplier;
use offs::now;
use offs::proto::filesystem as proto_types;
use offs::store::{DirEntity, FileDev, FileMode, FileType, Store};
use offs::store::id_generator::LocalTempIdGenerator;
use offs::store::wrapper::StoreWrapper;

use crate::remote_fs_client::client::grpc_client::RemoteFsGrpcClient;
use crate::remote_fs_client::client::modify_op_builder::ModifyOpBuilder;
use crate::remote_fs_client::error::{RemoteFsError, RemoteFsErrorKind};

use super::write_buffer::{WriteBuffer, WriteOperation};

pub type Result<T> = std::result::Result<T, RemoteFsError>;

macro_rules! err_offline {
    () => {
        return Err(RemoteFsError::new(RemoteFsErrorKind::Offline));
    };
}

macro_rules! check_online {
    ($self:ident) => {
        if $self.is_offline() {
            err_offline!();
        }
    };
}

pub struct OffsFilesystem {
    pub(super) client: RemoteFsGrpcClient,
    offline_mode: Arc<AtomicBool>,
    pub(super) should_flush_journal: Arc<AtomicBool>,

    next_inode: u64,
    pub(super) inodes_to_ids: HashMap<u64, String>,
    ids_to_inodes: HashMap<String, u64>,
    pub(super) store: StoreWrapper<LocalTempIdGenerator>,
    write_buffer: WriteBuffer,
}

impl OffsFilesystem {
    pub fn new(
        address: SocketAddr,
        offline_mode: Arc<AtomicBool>,
        should_flush_journal: Arc<AtomicBool>,
        store: Store<LocalTempIdGenerator>,
    ) -> Self {
        let mut fs = Self {
            client: RemoteFsGrpcClient::new(&format!("{}", address)),
            offline_mode,
            should_flush_journal,

            next_inode: 2,
            inodes_to_ids: [(1, "root".to_owned())].iter().cloned().collect(),
            ids_to_inodes: [("root".to_owned(), 1)].iter().cloned().collect(),
            store: StoreWrapper::new(store),
            write_buffer: WriteBuffer::new(),
        };

        // Request the root attributes
        fs.store.inner.create_default_root_directory();
        fs.update_dirent("root", true).unwrap();

        if !fs.is_offline() {
            fs.apply_journal();
        }

        fs
    }

    pub(super) fn get_inode_for_id(&mut self, id: &str) -> u64 {
        if !self.ids_to_inodes.contains_key(id) {
            self.ids_to_inodes.insert(id.to_owned(), self.next_inode);
            self.inodes_to_ids.insert(self.next_inode, id.to_owned());

            self.next_inode += 1;
        };

        self.ids_to_inodes[id]
    }

    pub(super) fn get_id_by_inode(&self, inode: u64) -> Result<&String> {
        self.inodes_to_ids
            .get(&inode)
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::NoEntry))
    }

    pub(super) fn query_file(&self, id: &str) -> Result<DirEntity> {
        self.store
            .inner
            .query_file(id)
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::NoEntry))
    }

    pub(super) fn query_file_by_name(&self, parent_id: &str, name: &str) -> Result<DirEntity> {
        self.store
            .inner
            .query_file_by_name(parent_id, name)
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::NoEntry))
    }

    pub(super) fn add_dirent(&mut self, mut dirent: &mut DirEntity) {
        let inode = self.get_inode_for_id(&dirent.id);
        dirent.stat.ino = inode;

        self.store.inner.add_or_replace_item(&dirent);
    }

    fn is_offline(&self) -> bool {
        self.offline_mode.load(Ordering::Relaxed)
    }

    // File operations

    // Read
    pub(super) fn update_dirent(&mut self, id: &str, update_atime: bool) -> Result<DirEntity> {
        let atime = if update_atime { Some(now()) } else { None };
        self.set_attributes(id, None, None, None, None, atime, None)
    }

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

    pub(super) fn update_chunks(&mut self, id: &str) -> Result<()> {
        if self.is_offline() {
            let dirent = self.store.inner.query_file(id).unwrap();
            if dirent.stat.size != 0 && !dirent.is_up_to_date() {
                err_offline!();
            }

            return Ok(());
        }

        let chunks = self.client.get_chunks(id);
        self.store
            .inner
            .replace_chunks(id, chunks.iter().enumerate());
        self.store.inner.update_retrieved_version(id);

        Ok(())
    }

    fn retrieve_missing_blobs(&mut self, ids: Vec<String>) -> Result<()> {
        if !ids.is_empty() {
            check_online!(self);

            let blobs = self.client.get_blobs(ids);

            let transaction = self.store.inner.transaction();
            for (_, blob) in &blobs {
                self.store.inner.add_blob(blob);
            }
            transaction.commit()?;
        };

        Ok(())
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
}

impl Drop for OffsFilesystem {
    fn drop(&mut self) {
        self.flush_write_buffer().unwrap();
    }
}
