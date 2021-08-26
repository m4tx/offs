use std::sync::atomic::Ordering;

use itertools::Itertools;
use prost::Message;

use offs::errors::{JournalApplyData, OperationApplyError};
use offs::modify_op::ModifyOperation;
use offs::proto::filesystem as proto_types;
use offs::store::id_generator::LocalTempIdGenerator;

use super::super::client::modify_op_builder::ModifyOpBuilder;
use super::OffsFilesystem;
use super::Result;

impl OffsFilesystem {
    async fn prepare_and_send_journal(&mut self) -> Result<JournalApplyData> {
        let blobs_used = self.store.inner.get_temp_chunks();
        let mut blob_ids_to_send = self.client.get_server_missing_blobs(blobs_used).await?;

        loop {
            let journal = self.store.inner.get_journal();
            if journal.is_empty() {
                return Ok(Default::default());
            }

            let ops: Vec<ModifyOperation> = journal
                .into_iter()
                .map(|x| {
                    let parsed = proto_types::ModifyOperation::decode(x.as_slice()).unwrap();

                    parsed.into()
                })
                .collect_vec();

            let chunks: Vec<Vec<String>> = self
                .store
                .inner
                .get_temp_file_ids()
                .map(|id| self.store.inner.get_chunks(&id))
                .collect();

            let blobs_to_send = self.store.inner.get_blobs(&blob_ids_to_send);

            let result = self
                .client
                .apply_journal(
                    ops,
                    chunks,
                    blobs_to_send.into_iter().map(|(_, v)| v).collect_vec(),
                )
                .await?;

            if result.is_ok() {
                return Ok(result.ok().unwrap());
            }

            match result.err().unwrap() {
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

                        let recreate_file_op =
                            ModifyOpBuilder::make_recreate_file_op(&parent_dirent, &dirent);
                        let recreate_file_op_proto: proto_types::ModifyOperation =
                            recreate_file_op.into();
                        self.store.inner.add_journal_entry(
                            &dirent.parent,
                            &recreate_file_op_proto.encode_to_vec(),
                        );

                        let reset_attributes_op =
                            ModifyOpBuilder::make_reset_attributes_op(&dirent);
                        let recreate_file_op_proto: proto_types::ModifyOperation =
                            reset_attributes_op.into();
                        self.store
                            .inner
                            .add_journal_entry(&new_id, &recreate_file_op_proto.encode_to_vec());
                    }

                    transaction.commit().unwrap();
                }
                OperationApplyError::MissingBlobs(mut ids) => {
                    blob_ids_to_send.append(&mut ids);
                }
            }
        }
    }

    pub(super) async fn apply_journal(&mut self) -> Result<()> {
        let JournalApplyData {
            assigned_ids,
            dir_entities,
        } = self.prepare_and_send_journal().await?;
        self.should_flush_journal.store(false, Ordering::Relaxed);

        if assigned_ids.is_empty() && dir_entities.is_empty() {
            return Ok(());
        }

        let transaction = self.store.inner.transaction();

        for (i, id) in assigned_ids.iter().enumerate() {
            self.store
                .inner
                .change_id(&LocalTempIdGenerator::get_nth_id(i), id);
        }
        for mut dirent in dir_entities {
            self.add_dirent(&mut dirent);
        }
        self.store.inner.clear_journal();

        transaction.commit().unwrap();

        Ok(())
    }
}
