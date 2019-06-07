use std::collections::HashMap;
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use capnp::serialize_packed;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use fuse::FileAttr;
use futures::Future;
use libc::{S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFREG, S_IFSOCK};
use time::Timespec;
use tokio::io::AsyncRead;

use offs::errors::OperationApplyError;
use offs::filesystem_capnp::modify_operation as ops;
use offs::filesystem_capnp::remote_fs_proto;
use offs::modify_op_handler::{OperationApplier, OperationHandler};
use offs::store::id_generator::LocalTempIdGenerator;
use offs::store::wrapper::StoreWrapper;
use offs::store::{DirEntity, FileDev, FileMode, FileType, Store};
use offs::{now, serialize_message};

use crate::remote_fs_client::client::modify_op_builder::ModifyOpBuilder;
use crate::remote_fs_client::error::{RemoteFsError, RemoteFsErrorKind};

use self::client::RemoteFsClient;
use self::write_buffer::{WriteBuffer, WriteOperation};

mod client;
mod error;
mod fuse_fs;
mod write_buffer;

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
    client: RemoteFsClient,
    offline_mode: Arc<AtomicBool>,
    should_flush_journal: Arc<AtomicBool>,

    next_inode: u64,
    inodes_to_ids: HashMap<u64, String>,
    ids_to_inodes: HashMap<String, u64>,
    store: StoreWrapper<LocalTempIdGenerator>,
    write_buffer: WriteBuffer,
}

impl OffsFilesystem {
    pub fn new(
        address: SocketAddr,
        offline_mode: Arc<AtomicBool>,
        should_flush_journal: Arc<AtomicBool>,
        store: Store<LocalTempIdGenerator>,
    ) -> Self {
        let (runtime, client) = Self::connect_rpc(address);

        let mut fs = Self {
            client: RemoteFsClient::new(runtime, client),
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

    fn connect_rpc(
        address: SocketAddr,
    ) -> (
        ::tokio::runtime::current_thread::Runtime,
        remote_fs_proto::Client,
    ) {
        let mut runtime = ::tokio::runtime::current_thread::Runtime::new().unwrap();

        let stream = runtime
            .block_on(::tokio::net::TcpStream::connect(&address))
            .unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.split();

        let network = Box::new(twoparty::VatNetwork::new(
            reader,
            std::io::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let mut rpc_system = RpcSystem::new(network, None);
        let client: remote_fs_proto::Client =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
        runtime.spawn(rpc_system.map_err(|_e| ()));

        (runtime, client)
    }

    fn convert_file_type(store_file_type: FileType) -> fuse::FileType {
        match store_file_type {
            FileType::NamedPipe => fuse::FileType::NamedPipe,
            FileType::CharDevice => fuse::FileType::CharDevice,
            FileType::BlockDevice => fuse::FileType::BlockDevice,
            FileType::Directory => fuse::FileType::Directory,
            FileType::RegularFile => fuse::FileType::RegularFile,
            FileType::Symlink => fuse::FileType::Symlink,
            FileType::Socket => fuse::FileType::Socket,
        }
    }

    fn mode_to_file_type(mode: u32) -> FileType {
        if (mode & S_IFIFO) == S_IFIFO {
            FileType::NamedPipe
        } else if (mode & S_IFCHR) == S_IFCHR {
            FileType::CharDevice
        } else if (mode & S_IFBLK) == S_IFBLK {
            FileType::BlockDevice
        } else if (mode & S_IFDIR) == S_IFDIR {
            FileType::Directory
        } else if (mode & S_IFREG) == S_IFREG {
            FileType::RegularFile
        } else if (mode & S_IFLNK) == S_IFLNK {
            FileType::Symlink
        } else if (mode & S_IFSOCK) == S_IFSOCK {
            FileType::Socket
        } else {
            unreachable!()
        }
    }

    fn get_inode_for_id(&mut self, id: &str) -> u64 {
        if !self.ids_to_inodes.contains_key(id) {
            self.ids_to_inodes.insert(id.to_owned(), self.next_inode);
            self.inodes_to_ids.insert(self.next_inode, id.to_owned());

            self.next_inode += 1;
        };

        self.ids_to_inodes[id]
    }

    fn get_id_by_inode(&self, inode: u64) -> Result<&String> {
        self.inodes_to_ids
            .get(&inode)
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::NoEntry))
    }

    fn query_file(&self, id: &str) -> Result<DirEntity> {
        self.store
            .inner
            .query_file(id)
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::NoEntry))
    }

    fn query_file_by_name(&self, parent_id: &str, name: &str) -> Result<DirEntity> {
        self.store
            .inner
            .query_file_by_name(parent_id, name)
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::NoEntry))
    }

    fn check_os_str(string: &OsStr) -> Result<&str> {
        string
            .to_str()
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::InvalidValue))
    }

    fn get_fuse_stat(&mut self, dirent: &DirEntity) -> FileAttr {
        let id = &dirent.id;
        let inode = self.get_inode_for_id(id);

        FileAttr {
            ino: inode,
            size: dirent.stat.size,
            blocks: dirent.stat.blocks,
            atime: dirent.stat.atim,
            mtime: dirent.stat.mtim,
            ctime: dirent.stat.ctim,
            crtime: Timespec { sec: 0, nsec: 0 },
            kind: OffsFilesystem::convert_file_type(dirent.stat.file_type),
            perm: dirent.stat.mode,
            nlink: dirent.stat.nlink as u32,
            uid: dirent.stat.uid,
            gid: dirent.stat.gid,
            rdev: dirent.stat.dev,
            flags: 0,
        }
    }

    fn add_dirent(&mut self, mut dirent: &mut DirEntity) {
        let inode = self.get_inode_for_id(&dirent.id);
        dirent.stat.ino = inode;

        self.store.inner.add_or_replace_item(&dirent);
    }

    fn is_offline(&self) -> bool {
        self.offline_mode.load(Ordering::Relaxed)
    }

    fn prepare_and_send_journal(&mut self) -> (Vec<String>, Vec<DirEntity>) {
        let blobs_used = self.store.inner.get_temp_chunks();
        let mut blob_ids_to_send = self.client.get_server_missing_blobs(blobs_used);

        loop {
            let journal = self.store.inner.get_journal();
            if journal.is_empty() {
                return (Default::default(), Default::default());
            }

            let readers: Vec<capnp::message::Reader<capnp::serialize::OwnedSegments>> = journal
                .into_iter()
                .map(|x| {
                    let mut vec_slice: &[u8] = &x;
                    serialize_packed::read_message(
                        &mut vec_slice,
                        ::capnp::message::ReaderOptions::new(),
                    )
                    .unwrap()
                })
                .collect();
            let ops: Vec<ops::Reader> = readers
                .iter()
                .map(|reader| reader.get_root::<ops::Reader>().unwrap())
                .collect();

            let chunks: Vec<Vec<String>> = self
                .store
                .inner
                .get_temp_file_ids()
                .map(|id| self.store.inner.get_chunks(&id))
                .collect();

            let blobs_to_send = self.store.inner.get_blobs(&blob_ids_to_send);

            let result = self
                .client
                .apply_journal(&ops, chunks, blobs_to_send.values());

            if result.is_ok() {
                return result.ok().unwrap();
            }

            match result.err().unwrap() {
                OperationApplyError::None => unreachable!(),
                OperationApplyError::InvalidJournal => {
                    panic!("The file operation journal is corrupted");
                }
                OperationApplyError::ConflictingFiles(ids) => {
                    let transaction = self.store.inner.transaction();

                    for id in ids {
                        self.store.inner.remove_file_from_journal(&id);
                        let new_id = self.store.inner.assign_temp_id(&id);

                        let dirent = self.store.inner.query_file(&new_id).unwrap();
                        let parent_dirent = self.store.inner.query_file(&dirent.parent).unwrap();

                        let mut message = Self::init_message();
                        ModifyOpBuilder::make_recreate_file_op(
                            &mut message,
                            &parent_dirent,
                            &dirent,
                        );
                        self.store
                            .inner
                            .add_journal_entry(&dirent.parent, &serialize_message(message));

                        let mut message = Self::init_message();
                        ModifyOpBuilder::make_reset_attributes_op(&mut message, &dirent);
                        self.store
                            .inner
                            .add_journal_entry(&new_id, &serialize_message(message));
                    }

                    transaction.commit().unwrap();
                }
                OperationApplyError::MissingBlobs(mut ids) => {
                    blob_ids_to_send.append(&mut ids);
                }
            }
        }
    }

    fn apply_journal(&mut self) {
        let (assigned_ids, dirents) = self.prepare_and_send_journal();
        self.should_flush_journal.store(false, Ordering::Relaxed);

        if assigned_ids.is_empty() && dirents.is_empty() {
            return;
        }

        let transaction = self.store.inner.transaction();

        for (i, id) in assigned_ids.iter().enumerate() {
            self.store
                .inner
                .change_id(&LocalTempIdGenerator::get_nth_id(i), id);
        }
        for mut dirent in dirents {
            self.add_dirent(&mut dirent);
        }
        self.store.inner.clear_journal();

        transaction.commit().unwrap();
    }

    // File operations

    // Read
    fn update_dirent(&mut self, id: &str, update_atime: bool) -> Result<DirEntity> {
        let atime = if update_atime { Some(now()) } else { None };
        self.set_attributes(id, None, None, None, None, atime, None)
    }

    fn list_files(&mut self, id: &str) -> Result<Vec<DirEntity>> {
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

    fn update_chunks(&mut self, id: &str) -> Result<()> {
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

    fn retrieve_missing_blobs<T: AsRef<str>>(&mut self, ids: &[T]) -> Result<()> {
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

    fn read(&mut self, id: &str, offset: i64, size: u32) -> Result<Vec<u8>> {
        let missing_blobs = self.store.get_missing_blobs_for_read(id, offset, size);
        self.retrieve_missing_blobs(&missing_blobs)?;

        Ok(self.store.read(id, offset, size))
    }

    // Modifications

    fn init_message() -> ::capnp::message::Builder<::capnp::message::HeapAllocator> {
        ::capnp::message::Builder::new_default()
    }

    fn apply_operation(&mut self, operation: ops::Reader) -> String {
        OperationApplier::apply_operation(self, operation)
            .ok()
            .unwrap()
    }

    fn perform_operation<A: ::capnp::message::Allocator>(
        &mut self,
        message: &capnp::message::Builder<A>,
    ) -> Result<DirEntity> {
        if self.should_flush_journal.load(Ordering::Relaxed) {
            self.apply_journal();
        }

        let operation: ops::Reader = message.get_root_as_reader().unwrap();
        let id = operation.get_id().unwrap();
        let transaction = self.store.inner.transaction();

        let new_id = self.apply_operation(operation);

        let mut serialized_op = Vec::new();
        serialize_packed::write_message(&mut serialized_op, &message).unwrap();

        let journal_entry_id = self.store.inner.add_journal_entry(id, &serialized_op);
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
    fn create_file(
        &mut self,
        parent_id: &str,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> Result<DirEntity> {
        let mut message = Self::init_message();
        let parent_dirent = self.query_file(parent_id)?;
        ModifyOpBuilder::make_create_file_op(
            &mut message,
            &parent_dirent,
            name,
            file_type,
            mode,
            dev,
        );

        let mut dirent = self.perform_operation(&message)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    fn create_symlink(&mut self, parent_id: &str, name: &str, link: &str) -> Result<DirEntity> {
        let mut message = Self::init_message();
        let parent_dirent = self.query_file(parent_id)?;
        ModifyOpBuilder::make_create_symlink_op(&mut message, &parent_dirent, name, link);

        let mut dirent = self.perform_operation(&message)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    fn create_directory(
        &mut self,
        parent_id: &str,
        name: &str,
        mode: FileMode,
    ) -> Result<DirEntity> {
        let mut message = Self::init_message();
        let parent_dirent = self.query_file(parent_id)?;
        ModifyOpBuilder::make_create_directory_op(&mut message, &parent_dirent, name, mode);

        let mut dirent = self.perform_operation(&message)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    // Remove
    fn remove_file(&mut self, id: &str) -> Result<()> {
        let mut message = Self::init_message();
        let dirent = self.query_file(id)?;
        ModifyOpBuilder::make_remove_file_op(&mut message, &dirent);

        self.perform_operation(&message)?;

        Ok(())
    }

    fn remove_directory(&mut self, id: &str) -> Result<()> {
        let mut message = Self::init_message();
        let dirent = self.query_file(id)?;
        ModifyOpBuilder::make_remove_directory_op(&mut message, &dirent);

        self.perform_operation(&message)?;

        Ok(())
    }

    // Modify
    fn rename_file(&mut self, id: &str, new_parent: &str, new_name: &str) -> Result<DirEntity> {
        let mut message = Self::init_message();
        let dirent = self.query_file(id)?;
        ModifyOpBuilder::make_rename_op(&mut message, &dirent, new_parent, new_name);

        let mut dirent = self.perform_operation(&message)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    fn set_attributes(
        &mut self,
        id: &str,
        mode: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
    ) -> Result<DirEntity> {
        let mut message = Self::init_message();
        let dirent = self.query_file(id)?;
        ModifyOpBuilder::make_set_attributes_op(
            &mut message,
            &dirent,
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
        );

        let mut dirent = self.perform_operation(&message)?;
        self.add_dirent(&mut dirent);

        Ok(dirent)
    }

    fn write(&mut self, id: &str, offset: i64, data: &[u8]) -> Result<()> {
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

    fn flush_write_buffer(&mut self) -> Result<()> {
        for op in self.write_buffer.flush().into_iter() {
            self.do_single_write(op)?;
        }

        Ok(())
    }

    fn do_single_write(&mut self, op: WriteOperation) -> Result<()> {
        let mut message = Self::init_message();
        let dirent = self.query_file(&op.id)?;
        ModifyOpBuilder::make_write_op(&mut message, &dirent, op.offset as i64, &op.data);

        let mut dirent = self.perform_operation(&message)?;
        self.add_dirent(&mut dirent);

        Ok(())
    }
}

impl OperationHandler for OffsFilesystem {
    fn perform_create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> String {
        self.store
            .create_file(parent_id, timestamp, name, file_type, mode, dev)
    }

    fn perform_create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        link: &str,
    ) -> String {
        self.store.create_symlink(parent_id, timestamp, name, link)
    }

    fn perform_create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        mode: FileMode,
    ) -> String {
        self.store
            .create_directory(parent_id, timestamp, name, mode)
    }

    fn perform_remove_file(&mut self, id: &str, timestamp: Timespec) {
        self.store.remove_file(id, timestamp);
    }

    fn perform_remove_directory(&mut self, id: &str, timestamp: Timespec) {
        self.store.remove_directory(id, timestamp);
    }

    fn perform_rename(&mut self, id: &str, timestamp: Timespec, new_parent: &str, new_name: &str) {
        self.store.rename(id, timestamp, new_parent, new_name);
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
        self.store
            .set_attributes(id, timestamp, mode, uid, gid, size, atim, mtim);
    }

    fn perform_write(&mut self, id: &str, timestamp: Timespec, offset: usize, data: &[u8]) {
        self.store.write(id, timestamp, offset, data);
    }
}

impl Drop for OffsFilesystem {
    fn drop(&mut self) {
        self.flush_write_buffer().unwrap();
    }
}
