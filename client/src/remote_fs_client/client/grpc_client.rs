use itertools::Itertools;

use crate::remote_fs_client::fs::Result;
use offs::errors::JournalApplyResult;
use offs::modify_op::ModifyOperation;
use offs::proto::filesystem as proto_types;
use offs::proto::filesystem::remote_fs_client::RemoteFsClient;
use offs::proto::filesystem::{
    ApplyJournalRequest, GetBlobsRequest, GetMissingBlobsRequest, ListChunksRequest, ListRequest,
};
use offs::store::DirEntity;

pub struct RemoteFsGrpcClient {
    client: RemoteFsClient<tonic::transport::Channel>,
}

impl RemoteFsGrpcClient {
    pub async fn new(address: &str) -> Result<Self> {
        let client = RemoteFsClient::connect(format!("http://{}", address))
            .await
            .unwrap();

        Ok(Self { client })
    }

    // Listing
    pub async fn list_files(&mut self, dir_id: &str) -> Result<Vec<DirEntity>> {
        let req = ListRequest {
            id: dir_id.to_owned(),
        };

        let mut stream = self.client.list(req).await?.into_inner();
        let mut res: Vec<DirEntity> = Vec::new();

        while let Some(dir_entity) = stream.message().await? {
            res.push(dir_entity.into());
        }

        Ok(res)
    }

    pub async fn get_chunks(&mut self, id: &str) -> Result<Vec<String>> {
        let req = ListChunksRequest { id: id.to_owned() };

        let resp = self.client.list_chunks(req).await?.into_inner();
        Ok(resp.blob_id)
    }

    pub async fn get_blobs(&mut self, ids: Vec<String>) -> Result<Vec<(String, Vec<u8>)>> {
        let req = GetBlobsRequest { id: ids.into() };

        let mut stream = self.client.get_blobs(req).await?.into_inner();
        let mut res = Vec::new();

        while let Some(blob) = stream.message().await? {
            res.push((blob.id, blob.content));
        }

        Ok(res)
    }

    // Modifications
    pub async fn request_apply_operation(
        &mut self,
        modify_operation: ModifyOperation,
    ) -> Result<DirEntity> {
        let result = self
            .client
            .apply_operation(offs::proto::filesystem::ModifyOperation::from(
                modify_operation,
            ))
            .await?
            .into_inner();

        Ok(result.into())
    }

    pub async fn apply_journal<'a>(
        &mut self,
        journal: Vec<ModifyOperation>,
        chunks: Vec<Vec<String>>,
        blobs: Vec<Vec<u8>>,
    ) -> Result<JournalApplyResult> {
        let converted_journal: Vec<proto_types::ModifyOperation> =
            journal.into_iter().map(|x| x.into()).collect_vec();
        let converted_chunks: Vec<proto_types::FileChunks> =
            chunks.into_iter().map(|x| x.into()).collect_vec();

        let req = ApplyJournalRequest {
            operations: converted_journal.into(),
            chunks: converted_chunks.into(),
            blobs: blobs.into(),
        };

        let result = self.client.apply_journal(req).await?.into_inner();
        Ok(result.into())
    }

    pub async fn get_server_missing_blobs(&mut self, ids: Vec<String>) -> Result<Vec<String>> {
        let req = GetMissingBlobsRequest { id: ids.into() };

        let result = self.client.get_missing_blobs(req).await?.into_inner();
        Ok(result.blob_id)
    }
}
