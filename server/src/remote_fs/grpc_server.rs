use futures::sink::Sink;
use futures::stream;
use futures::Future;
use grpcio::{Error, RpcContext, ServerStreamingSink, UnarySink, WriteFlags};
use itertools::Itertools;

use offs::modify_op;
use offs::modify_op_handler::OperationApplier;
use offs::proto::filesystem::{
    ApplyJournalRequest, ApplyJournalResponse, Blob, DirEntity, GetBlobsRequest,
    GetMissingBlobsRequest, GetMissingBlobsResult, ListChunksRequest, ListChunksResult,
    ListRequest, ModifyOperation,
};
use offs::proto::filesystem_grpc::RemoteFs;

impl RemoteFs for super::RemoteFs {
    fn list(&mut self, ctx: RpcContext, req: ListRequest, sink: ServerStreamingSink<DirEntity>) {
        let files = self
            .store
            .inner
            .list_files(&req.id)
            .into_iter()
            .map(|x| (DirEntity::from(x), WriteFlags::default()));

        let f = sink
            .send_all(stream::iter_ok::<_, Error>(files))
            .map(|_| {})
            .map_err(|e| println!("failed to handle List request: {:?}", e));
        ctx.spawn(f)
    }

    fn list_chunks(
        &mut self,
        ctx: RpcContext,
        req: ListChunksRequest,
        sink: UnarySink<ListChunksResult>,
    ) {
        let chunks = self.store.inner.get_chunks(&req.id);

        let mut resp = ListChunksResult::default();
        resp.set_blob_id(chunks.into());

        let f = sink
            .success(resp)
            .map_err(|e| println!("failed to handle ListChunks request: {:?}", e));
        ctx.spawn(f)
    }

    fn get_blobs(
        &mut self,
        ctx: RpcContext,
        req: GetBlobsRequest,
        sink: ServerStreamingSink<Blob>,
    ) {
        let blobs = self
            .store
            .inner
            .get_blobs(req.id.into_vec())
            .into_iter()
            .map(|(k, v)| {
                let mut blob = Blob::default();

                blob.set_id(k);
                blob.set_content(v);

                (blob, WriteFlags::default())
            });

        let f = sink
            .send_all(stream::iter_ok::<_, Error>(blobs))
            .map(|_| {})
            .map_err(|e| println!("failed to handle GetBlobs request: {:?}", e));
        ctx.spawn(f)
    }

    fn apply_operation(
        &mut self,
        ctx: RpcContext,
        req: ModifyOperation,
        sink: UnarySink<DirEntity>,
    ) {
        let transaction = self.store.inner.transaction();

        let new_id = OperationApplier::apply_operation(self, &req.into())
            .ok()
            .unwrap();

        let dir_entity = self.store.inner.query_file(&new_id).unwrap();

        transaction.commit().unwrap();

        let f = sink
            .success(dir_entity.into())
            .map_err(|e| println!("failed to handle ApplyOperation request: {:?}", e));
        ctx.spawn(f)
    }

    fn apply_journal(
        &mut self,
        ctx: RpcContext,
        req: ApplyJournalRequest,
        sink: UnarySink<ApplyJournalResponse>,
    ) {
        let converted_operations: Vec<modify_op::ModifyOperation> =
            req.operations.into_iter().map(|x| x.into()).collect_vec();
        let converted_chunks: Vec<Vec<String>> =
            req.chunks.into_iter().map(|x| x.into()).collect_vec();
        let converted_blobs: Vec<Vec<u8>> = req.blobs.into();

        let transaction = self.store.inner.transaction();

        let result =
            self.apply_full_journal(converted_operations, converted_chunks, converted_blobs);
        if result.is_ok() {
            transaction.commit().unwrap();
        }

        let f = sink
            .success(result.into())
            .map_err(|e| println!("failed to handle ApplyJournal request: {:?}", e));
        ctx.spawn(f)
    }

    fn get_missing_blobs(
        &mut self,
        ctx: RpcContext,
        req: GetMissingBlobsRequest,
        sink: UnarySink<GetMissingBlobsResult>,
    ) {
        let chunks = self.store.inner.get_missing_blobs(req.id.into_vec());

        let mut resp = GetMissingBlobsResult::default();
        resp.set_blob_id(chunks.into());

        let f = sink
            .success(resp)
            .map_err(|e| println!("failed to handle GetMissingBlobs request: {:?}", e));
        ctx.spawn(f)
    }
}
