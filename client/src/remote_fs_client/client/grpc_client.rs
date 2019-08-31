use std::sync::Arc;

use futures::stream::Stream;
use futures::Future;
use grpcio::{ChannelBuilder, EnvBuilder};
use itertools::Itertools;
use time::Timespec;

use offs::errors::JournalApplyResult;
use offs::modify_op::ModifyOperation;
use offs::proto::filesystem as proto_types;
use offs::proto::filesystem::{
    ApplyJournalRequest, GetBlobsRequest, GetMissingBlobsRequest, ListChunksRequest, ListRequest,
};
use offs::proto::filesystem_grpc::RemoteFsClient;
use offs::store::{DirEntity, FileStat, FileType};

pub struct RemoteFsGrpcClient {
    client: RemoteFsClient,
}

impl RemoteFsGrpcClient {
    pub fn new(address: &str) -> Self {
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(address);
        let client = RemoteFsClient::new(ch);

        Self { client }
    }

    // Listing
    pub fn list_files(&mut self, dir_id: &str) -> Vec<DirEntity> {
        let mut req = ListRequest::default();
        req.set_id(dir_id.to_owned());

        let mut resp = self.client.list(&req).unwrap();
        let mut res = Vec::new();

        loop {
            let f = resp.into_future();
            match f.wait() {
                Ok((Some(feature), s)) => {
                    resp = s;

                    let stat = FileStat {
                        ino: feature.get_stat().ino,
                        file_type: FileType::RegularFile,
                        mode: feature.get_stat().perm as u16,
                        dev: 0,
                        nlink: feature.get_stat().nlink,
                        uid: feature.get_stat().uid,
                        gid: feature.get_stat().gid,
                        size: feature.get_stat().size,
                        blocks: feature.get_stat().blocks,
                        atim: Timespec::new(
                            feature.get_stat().get_atim().sec,
                            feature.get_stat().get_atim().nsec,
                        ),
                        mtim: Timespec::new(
                            feature.get_stat().get_mtim().sec,
                            feature.get_stat().get_mtim().nsec,
                        ),
                        ctim: Timespec::new(
                            feature.get_stat().get_ctim().sec,
                            feature.get_stat().get_ctim().nsec,
                        ),
                    };
                    let dir_entity = DirEntity {
                        id: feature.id,
                        parent: feature.parent,
                        name: feature.name,
                        dirent_version: feature.dirent_version,
                        content_version: feature.content_version,
                        retrieved_version: feature.content_version,
                        stat: stat,
                    };

                    res.push(dir_entity);
                }
                Ok((None, _)) => break,
                Err((e, _)) => panic!("List files failed: {:?}", e),
            }
        }

        res
    }

    pub fn get_chunks(&mut self, id: &str) -> Vec<String> {
        let mut req = ListChunksRequest::default();
        req.set_id(id.to_owned());

        let resp = self.client.list_chunks(&req).unwrap();
        resp.blob_id.into_vec()
    }

    pub fn get_blobs(&mut self, ids: Vec<String>) -> Vec<(String, Vec<u8>)> {
        let mut req = GetBlobsRequest::default();
        req.set_id(ids.into());

        let mut resp = self.client.get_blobs(&req).unwrap();
        let mut res = Vec::new();

        loop {
            let f = resp.into_future();
            match f.wait() {
                Ok((Some(feature), s)) => {
                    resp = s;

                    res.push((feature.id, feature.content));
                }
                Ok((None, _)) => break,
                Err((e, _)) => panic!("Get blobs failed: {:?}", e),
            }
        }

        res
    }

    // Modifications
    pub fn request_apply_operation(&mut self, modify_operation: ModifyOperation) -> DirEntity {
        let result = self
            .client
            .apply_operation(&modify_operation.into())
            .unwrap();

        result.into()
    }

    pub fn apply_journal<'a>(
        &mut self,
        journal: Vec<ModifyOperation>,
        chunks: Vec<Vec<String>>,
        blobs: Vec<Vec<u8>>,
    ) -> JournalApplyResult {
        let converted_journal: Vec<proto_types::ModifyOperation> =
            journal.into_iter().map(|x| x.into()).collect_vec();
        let converted_chunks: Vec<proto_types::FileChunks> =
            chunks.into_iter().map(|x| x.into()).collect_vec();

        let mut req = ApplyJournalRequest::default();
        req.set_operations(converted_journal.into());
        req.set_chunks(converted_chunks.into());
        req.set_blobs(blobs.into());

        let result = self.client.apply_journal(&req).unwrap();
        result.into()
    }

    pub fn get_server_missing_blobs(&mut self, ids: Vec<String>) -> Vec<String> {
        let mut req = GetMissingBlobsRequest::default();
        req.set_id(ids.into());

        let result = self.client.get_missing_blobs(&req).unwrap();
        result.blob_id.into_vec()
    }
}
