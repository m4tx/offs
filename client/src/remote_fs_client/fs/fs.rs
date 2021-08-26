use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use offs::now;
use offs::store::id_generator::LocalTempIdGenerator;
use offs::store::wrapper::StoreWrapper;
use offs::store::{DirEntity, Store};

use super::super::client::grpc_client::RemoteFsGrpcClient;
use super::error::{RemoteFsError, RemoteFsErrorKind};
use super::write_buffer::WriteBuffer;

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
    pub(super) write_buffer: WriteBuffer,
}

impl OffsFilesystem {
    pub async fn new(
        address: SocketAddr,
        offline_mode: Arc<AtomicBool>,
        should_flush_journal: Arc<AtomicBool>,
        store: Store<LocalTempIdGenerator>,
    ) -> Result<Self> {
        let mut fs = Self {
            client: RemoteFsGrpcClient::new(&format!("{}", address)).await?,
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
        fs.update_dirent("root", true).await?;

        if !fs.is_offline() {
            fs.apply_journal().await?;
        }

        Ok(fs)
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

    pub(super) async fn update_dirent(
        &mut self,
        id: &str,
        update_atime: bool,
    ) -> Result<DirEntity> {
        let atime = if update_atime { Some(now()) } else { None };
        self.set_attributes(id, None, None, None, None, atime, None)
            .await
    }

    pub(super) async fn retrieve_missing_blobs(&mut self, ids: Vec<String>) -> Result<()> {
        if !ids.is_empty() {
            check_online!(self);

            let blobs = self.client.get_blobs(ids).await?;

            let transaction = self.store.inner.transaction();
            for (_, blob) in &blobs {
                self.store.inner.add_blob(blob);
            }
            transaction.commit()?;
        };

        Ok(())
    }

    pub(super) async fn update_chunks(&mut self, id: &str) -> Result<()> {
        if self.is_offline() {
            let dirent = self.store.inner.query_file(id).unwrap();
            if dirent.stat.size != 0 && !dirent.is_up_to_date() {
                err_offline!();
            }

            return Ok(());
        }

        let chunks = self.client.get_chunks(id).await?;
        self.store
            .inner
            .replace_chunks(id, chunks.iter().enumerate());
        self.store.inner.update_retrieved_version(id);

        Ok(())
    }

    pub(super) fn is_offline(&self) -> bool {
        self.offline_mode.load(Ordering::Relaxed)
    }
}
