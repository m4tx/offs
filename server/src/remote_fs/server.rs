use capnp::capability::Promise;
use capnp_rpc::pry;
use itertools::Itertools;

use offs::filesystem_capnp::remote_fs_proto;
use offs::list_list_text_to_vec;
use offs::modify_op_handler::OperationApplier;
use offs::store::ProtoFill;

use super::RemoteFs;

impl remote_fs_proto::Server for RemoteFs {
    fn list(
        &mut self,
        params: remote_fs_proto::ListParams,
        mut results: remote_fs_proto::ListResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());

        let files = self.store.inner.list_files(params.get_id().unwrap());

        let mut list = results.get().init_list(files.len() as u32);
        for (i, item) in files.iter().enumerate() {
            let dirent = list.reborrow().get(i as u32);
            item.fill_proto(dirent);
        }

        Promise::ok(())
    }

    fn list_chunks(
        &mut self,
        params: remote_fs_proto::ListChunksParams,
        mut results: remote_fs_proto::ListChunksResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());

        let chunks = self.store.inner.get_chunks(params.get_id().unwrap());

        let mut list = results.get().init_list(chunks.len() as u32);
        for (i, item) in chunks.iter().enumerate() {
            list.set(i as u32, item);
        }

        Promise::ok(())
    }

    fn get_blobs(
        &mut self,
        params: remote_fs_proto::GetBlobsParams,
        mut results: remote_fs_proto::GetBlobsResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());

        let blobs = self
            .store
            .inner
            .get_blobs(params.get_ids().unwrap().iter().map(|x| x.unwrap()));

        let mut list = results.get().init_list(blobs.len() as u32);
        for (i, (id, content)) in blobs.iter().enumerate() {
            let mut item = list.reborrow().get(i as u32);

            item.set_id(id);
            item.set_content(content);
        }

        Promise::ok(())
    }

    fn apply_operation(
        &mut self,
        params: remote_fs_proto::ApplyOperationParams,
        mut results: remote_fs_proto::ApplyOperationResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let operation = params.get_operation().unwrap();

        let transaction = self.store.inner.transaction();

        let new_id = OperationApplier::apply_operation(self, operation)
            .ok()
            .unwrap();

        let dirent = self.store.inner.query_file(&new_id);

        transaction.commit().unwrap();

        if dirent.is_some() {
            let proto_dirent = results.get().init_dir_entity();
            dirent.unwrap().fill_proto(proto_dirent);
        }

        Promise::ok(())
    }

    fn apply_journal(
        &mut self,
        params: remote_fs_proto::ApplyJournalParams,
        mut results: remote_fs_proto::ApplyJournalResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let operation_list = params.get_list().unwrap().iter();
        let chunks = params.get_chunks().unwrap();
        let chunks_vec = list_list_text_to_vec(chunks);
        let blobs = params.get_blobs().unwrap().iter();

        let transaction = self.store.inner.transaction();

        let result = self.apply_full_journal(operation_list, chunks_vec, blobs.map(|x| x.unwrap()));

        if let Ok((assigned_ids, processed_ids)) = result {
            let dirents = processed_ids
                .iter()
                .filter_map(|id| self.store.inner.query_file(id))
                .collect_vec();

            let mut assigned_id_list = results.get().init_assigned_ids(assigned_ids.len() as u32);
            for (i, item) in assigned_ids.iter().enumerate() {
                assigned_id_list.set(i as u32, item);
            }
            let mut dirent_list = results.get().init_dir_entities(dirents.len() as u32);
            for (i, dirent) in dirents.iter().enumerate() {
                dirent.fill_proto(dirent_list.reborrow().get(i as u32));
            }

            transaction.commit().unwrap();
        } else if let Err(err) = result {
            err.fill_proto(results.get().init_error());
        };

        Promise::ok(())
    }

    fn get_missing_blobs(
        &mut self,
        params: remote_fs_proto::GetMissingBlobsParams,
        mut results: remote_fs_proto::GetMissingBlobsResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());

        let list = params.get_ids().unwrap().iter().map(|x| x.unwrap());
        let blobs = self.store.inner.get_missing_blobs(list);

        let mut id_list = results.get().init_list(blobs.len() as u32);
        for (i, id) in blobs.iter().enumerate() {
            id_list.set(i as u32, id);
        }

        Promise::ok(())
    }
}
