use std::ops::DerefMut;

use itertools::Itertools;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use offs::modify_op;
use offs::modify_op::ModifyOperationContent;
use offs::modify_op_handler::OperationApplier;
use offs::proto::filesystem::remote_fs_server::RemoteFs;
use offs::proto::filesystem::{
    ApplyJournalRequest, ApplyJournalResponse, Blob, DirEntity, GetBlobsRequest,
    GetMissingBlobsRequest, GetMissingBlobsResult, ListChunksRequest, ListChunksResult,
    ListRequest, ModifyOperation,
};

pub struct RemoteFsServerImpl {
    fs: RwLock<super::RemoteFs>,
}

impl RemoteFsServerImpl {
    pub fn new(fs: super::RemoteFs) -> Self {
        Self {
            fs: RwLock::new(fs),
        }
    }
}

#[tonic::async_trait]
impl RemoteFs for RemoteFsServerImpl {
    type ListStream = ReceiverStream<Result<DirEntity, Status>>;

    async fn list(
        &self,
        request: Request<ListRequest>,
    ) -> Result<Response<Self::ListStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let files = self
            .fs
            .read()
            .await
            .store
            .list_files(&request.into_inner().id)?
            .into_iter()
            .map(|x| DirEntity::from(x));

        tokio::spawn(async move {
            for file in files {
                tx.send(Ok(file)).await.unwrap();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResult>, Status> {
        let chunks = self
            .fs
            .read()
            .await
            .store
            .get_chunks(&request.into_inner().id)?;

        let resp = ListChunksResult {
            blob_id: chunks.into(),
        };

        Ok(Response::new(resp))
    }

    type GetBlobsStream = ReceiverStream<Result<Blob, Status>>;

    async fn get_blobs(
        &self,
        request: Request<GetBlobsRequest>,
    ) -> Result<Response<Self::GetBlobsStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let blobs = self
            .fs
            .read()
            .await
            .store
            .get_blobs(request.into_inner().id)?
            .into_iter()
            .map(|(k, v)| Blob { id: k, content: v });

        tokio::spawn(async move {
            for blob in blobs {
                tx.send(Ok(blob)).await.unwrap();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn apply_operation(
        &self,
        request: Request<ModifyOperation>,
    ) -> Result<Response<DirEntity>, Status> {
        let dir_entity = {
            let mut fs = self.fs.write().await;
            let transaction = fs.store.transaction();

            let operation: offs::modify_op::ModifyOperation = request.into_inner().into();
            let dir_entity = fs.store.try_query_file(&operation.id)?;

            let new_id = OperationApplier::apply_operation(fs.deref_mut(), &operation)?;

            let dir_entity = match operation.operation {
                ModifyOperationContent::RemoveFileOperation(_)
                | ModifyOperationContent::RemoveDirectoryOperation(_) => dir_entity.unwrap(),
                _ => fs.store.query_file(&new_id)?,
            };

            transaction.commit().unwrap();

            dir_entity
        };

        Ok(Response::new(dir_entity.into()))
    }

    async fn apply_journal(
        &self,
        request: Request<ApplyJournalRequest>,
    ) -> Result<Response<ApplyJournalResponse>, Status> {
        let req = request.into_inner();
        let converted_operations: Vec<modify_op::ModifyOperation> =
            req.operations.into_iter().map(|x| x.into()).collect_vec();
        let converted_chunks: Vec<Vec<String>> =
            req.chunks.into_iter().map(|x| x.into()).collect_vec();
        let converted_blobs: Vec<Vec<u8>> = req.blobs.into();

        let result = {
            let mut fs = self.fs.write().await;
            let transaction = fs.store.transaction();

            let result =
                fs.apply_full_journal(converted_operations, converted_chunks, converted_blobs);
            if result.is_ok() {
                transaction.commit().unwrap();
            }

            result
        };

        Ok(Response::new(result.into()))
    }

    async fn get_missing_blobs(
        &self,
        request: Request<GetMissingBlobsRequest>,
    ) -> Result<Response<GetMissingBlobsResult>, Status> {
        let chunks = self
            .fs
            .read()
            .await
            .store
            .get_missing_blobs(request.into_inner().id)?;

        let resp = GetMissingBlobsResult {
            blob_id: chunks.into(),
        };

        Ok(Response::new(resp))
    }
}
