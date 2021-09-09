use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use offs::store::id_generator::LocalTempIdGenerator;
use offs::store::wrapper::StoreWrapper;
use offs::store::{DirEntity, Store};
use offs::{now, ROOT_ID};

use super::super::client::grpc_client::RemoteFsGrpcClient;
use crate::remote_fs_client::fs::open_file_handler::OpenFileHandler;
use offs::errors::{OperationError, OperationResult};

macro_rules! err_offline {
    () => {
        return Err(OperationError::offline("The client is currently offline"));
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

    pub(super) store: StoreWrapper<LocalTempIdGenerator>,
    pub(super) open_file_handler: OpenFileHandler,
}

impl OffsFilesystem {
    pub async fn new(
        address: SocketAddr,
        offline_mode: Arc<AtomicBool>,
        should_flush_journal: Arc<AtomicBool>,
        store: Store<LocalTempIdGenerator>,
    ) -> OperationResult<Self> {
        let mut fs = Self {
            client: RemoteFsGrpcClient::new(&format!("{}", address)).await?,
            offline_mode,
            should_flush_journal,

            store: StoreWrapper::new(store),
            open_file_handler: OpenFileHandler::new(),
        };

        // Request the root attributes
        fs.store.create_default_root_directory()?;
        fs.update_dirent(ROOT_ID, true).await?;

        if !fs.is_offline() {
            fs.apply_journal().await?;
        }

        Ok(fs)
    }

    pub(super) fn add_dirent(&mut self, dirent: &mut DirEntity) -> OperationResult<()> {
        self.store.add_or_replace_dirent(&dirent)?;
        Ok(())
    }

    pub(super) async fn update_dirent(
        &mut self,
        id: &str,
        update_atime: bool,
    ) -> OperationResult<DirEntity> {
        let atime = if update_atime { Some(now()) } else { None };
        self.set_attributes(id, None, None, None, None, atime, None)
            .await
    }

    pub(super) async fn retrieve_missing_blobs(&mut self, ids: Vec<String>) -> OperationResult<()> {
        if !ids.is_empty() {
            check_online!(self);

            let blobs = self.client.get_blobs(ids).await?;

            let transaction = self.store.transaction();
            for (_, blob) in &blobs {
                self.store.add_blob(blob)?;
            }
            transaction.commit()?;
        };

        Ok(())
    }

    pub(super) async fn update_chunks(&mut self, id: &str) -> OperationResult<()> {
        if self.is_offline() {
            let dirent = self.store.query_file(id)?;
            if dirent.stat.size != 0 && !dirent.is_up_to_date() {
                err_offline!();
            }

            return Ok(());
        }

        let chunks = self.client.get_chunks(id).await?;
        self.store.replace_chunks(id, chunks.iter().enumerate())?;
        self.store.update_retrieved_version(id)?;

        Ok(())
    }

    pub(super) fn is_offline(&self) -> bool {
        self.offline_mode.load(Ordering::Relaxed)
    }
}
