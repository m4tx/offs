use capnp::capability::Promise;
use capnp_rpc::pry;
use futures::Future;

use offs::errors::OperationApplyError;
use offs::filesystem_capnp::modify_operation as ops;
use offs::filesystem_capnp::remote_fs_proto;
use offs::store::DirEntity;
use std::result;

pub mod modify_op_builder;

pub struct RemoteFsClient {
    runtime: ::tokio::runtime::current_thread::Runtime,
    client: remote_fs_proto::Client,
}

type Result<T> = result::Result<T, OperationApplyError>;

impl RemoteFsClient {
    pub fn new(
        runtime: ::tokio::runtime::current_thread::Runtime,
        client: remote_fs_proto::Client,
    ) -> Self {
        Self { runtime, client }
    }

    // Listing
    pub fn list_files(&mut self, dir_id: &str) -> Vec<DirEntity> {
        let mut request = self.client.list_request();
        request.get().set_id(dir_id);

        self.runtime
            .block_on(request.send().promise.and_then(|response| {
                let list = pry!(response.get()).get_list().unwrap();
                let mut v: Vec<DirEntity> = Vec::new();

                for item in list.iter() {
                    v.push(item.into());
                }

                Promise::ok(v)
            }))
            .unwrap()
    }

    pub fn get_chunks(&mut self, id: &str) -> Vec<String> {
        let mut request = self.client.list_chunks_request();
        request.get().set_id(&id);

        self.runtime
            .block_on(request.send().promise.and_then(|response| {
                let list = pry!(response.get()).get_list().unwrap();
                let mut vec = Vec::new();

                for item in list {
                    vec.push(item.unwrap().to_owned());
                }

                Promise::ok(vec)
            }))
            .unwrap()
    }

    pub fn get_blobs<T: IntoIterator>(&mut self, ids: T) -> Vec<(String, Vec<u8>)>
    where
        T::Item: AsRef<str>,
        T::IntoIter: ExactSizeIterator,
    {
        let iter = ids.into_iter();

        let mut request = self.client.get_blobs_request();
        let mut id_list = request.get().init_ids(iter.len() as u32);
        for (i, item) in iter.enumerate() {
            id_list.set(i as u32, item.as_ref());
        }

        self.runtime
            .block_on(request.send().promise.and_then(|response| {
                let list = pry!(response.get()).get_list().unwrap();
                let mut vec = Vec::new();

                for item in list {
                    vec.push((
                        item.get_id().unwrap().to_owned(),
                        Vec::from(item.get_content().unwrap()),
                    ));
                }

                Promise::ok(vec)
            }))
            .unwrap()
    }

    // Modifications
    pub fn request_apply_operation(&mut self, modify_operation: ops::Reader) -> DirEntity {
        let mut request = self.client.apply_operation_request();
        request.get().set_operation(modify_operation).unwrap();

        self.runtime
            .block_on(request.send().promise.and_then(|response| {
                let proto_dirent = pry!(response.get()).get_dir_entity().unwrap();
                Promise::ok(DirEntity::from(proto_dirent))
            }))
            .unwrap()
    }

    pub fn apply_journal<'a>(
        &mut self,
        journal: &[ops::Reader],
        chunks: Vec<Vec<String>>,
        blobs: impl ExactSizeIterator<Item = &'a Vec<u8>>,
    ) -> Result<(Vec<String>, Vec<DirEntity>)> {
        let mut request = self.client.apply_journal_request();

        let op_list = request.get().init_list(journal.len() as u32);
        for (i, item) in journal.iter().enumerate() {
            op_list.set_with_caveats(i as u32, *item).unwrap();
        }

        let mut chunk_list = request.get().init_chunks(chunks.len() as u32);
        for (i, chunks) in chunks.iter().enumerate() {
            let mut list = chunk_list.reborrow().init(i as u32, chunks.len() as u32);
            for (j, id) in chunks.iter().enumerate() {
                list.set(j as u32, id);
            }
        }

        let mut blob_list = request.get().init_blobs(blobs.len() as u32);
        for (i, blob) in blobs.enumerate() {
            blob_list.set(i as u32, blob);
        }

        self.runtime
            .block_on(request.send().promise.and_then(|response| {
                let response = pry!(response.get());
                let assigned_ids_proto = response.get_assigned_ids().unwrap();
                let dirents_proto = response.get_dir_entities().unwrap();

                let assigned_ids: Vec<String> = assigned_ids_proto
                    .iter()
                    .map(|x| x.unwrap().to_owned())
                    .collect();
                let dirents: Vec<DirEntity> = dirents_proto.iter().map(|x| x.into()).collect();

                let error: OperationApplyError = response.get_error().unwrap().into();
                if error == OperationApplyError::None {
                    Promise::ok(Ok((assigned_ids, dirents)))
                } else {
                    Promise::ok(Err(error))
                }
            }))
            .unwrap()
    }

    pub fn get_server_missing_blobs<T: IntoIterator>(&mut self, ids: T) -> Vec<String>
    where
        T::Item: AsRef<str>,
        T::IntoIter: ExactSizeIterator,
    {
        let iter = ids.into_iter();
        let mut request = self.client.get_missing_blobs_request();

        let mut id_list = request.get().init_ids(iter.len() as u32);
        for (i, item) in iter.enumerate() {
            id_list.set(i as u32, item.as_ref());
        }

        self.runtime
            .block_on(request.send().promise.and_then(|response| {
                let response = pry!(response.get());
                let id_list = response.get_list().unwrap();

                let ids: Vec<String> = id_list.iter().map(|x| x.unwrap().to_owned()).collect();
                Promise::ok(ids)
            }))
            .unwrap()
    }
}
