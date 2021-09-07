use std::sync::atomic::Ordering;

use itertools::Itertools;
use prost::Message;

use offs::errors::{JournalApplyData, JournalApplyError};
use offs::modify_op::ModifyOperation;
use offs::proto::filesystem as proto_types;
use offs::store::id_generator::LocalTempIdGenerator;

use super::super::client::modify_op_builder::ModifyOpBuilder;
use super::OffsFilesystem;
use super::Result;

const JOURNAL_MAX_RETRIES: u32 = 10;

impl OffsFilesystem {
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
                .change_id(&LocalTempIdGenerator::get_nth_id(i), id)?;
        }
        for mut dirent in dir_entities {
            self.add_dirent(&mut dirent)?;
        }
        self.store.inner.clear_journal()?;

        transaction.commit().unwrap();

        Ok(())
    }

    async fn prepare_and_send_journal(&mut self) -> Result<JournalApplyData> {
        for _ in 0..JOURNAL_MAX_RETRIES {
            let result = self.try_prepare_and_send_journal().await?;
            if let Some(journal_apply_data) = result {
                return Ok(journal_apply_data);
            }
        }

        panic!(
            "Could not apply journal after {} tries",
            JOURNAL_MAX_RETRIES
        );
    }

    async fn try_prepare_and_send_journal(&mut self) -> Result<Option<JournalApplyData>> {
        let ops = self.prepare_ops_to_send()?;
        if ops.is_empty() {
            return Ok(Some(Default::default()));
        }
        let chunks = self.prepare_chunks_to_send()?;
        let blobs = self.prepare_blobs_to_send().await?;

        let result = self.client.apply_journal(ops, chunks, blobs).await?;

        if let Ok(result_ok) = result {
            return Ok(Some(result_ok));
        }

        match result.err().unwrap() {
            JournalApplyError::InvalidJournal => {
                panic!("The file operation journal is corrupted");
            }
            JournalApplyError::ConflictingFiles(ids) => {
                self.recreate_conflicting_files(ids)?;
            }
            JournalApplyError::MissingBlobs(_) => {
                // Do nothing, we will re-query the missing blobs in the next iteration
            }
        }

        Ok(None)
    }

    fn prepare_ops_to_send(&mut self) -> Result<Vec<ModifyOperation>> {
        Ok(self
            .store
            .inner
            .get_journal()?
            .into_iter()
            .map(|x| {
                let parsed = proto_types::ModifyOperation::decode(x.as_slice()).unwrap();
                parsed.into()
            })
            .collect_vec())
    }

    fn prepare_chunks_to_send(&mut self) -> Result<Vec<Vec<String>>> {
        Ok(self
            .store
            .inner
            .get_temp_file_ids()
            .map(|id| self.store.inner.get_chunks(&id).unwrap())
            .collect())
    }

    async fn prepare_blobs_to_send(&mut self) -> Result<Vec<Vec<u8>>> {
        let blobs_used = self.store.inner.get_temp_chunks()?;
        let blob_ids_to_send = self.client.get_server_missing_blobs(blobs_used).await?;
        let blobs_to_send = self.store.inner.get_blobs(&blob_ids_to_send)?;
        Ok(blobs_to_send.into_iter().map(|(_, v)| v).collect_vec())
    }

    fn recreate_conflicting_files(&mut self, ids: Vec<String>) -> Result<()> {
        let transaction = self.store.inner.transaction();

        for id in ids {
            self.recreate_conflicting_file(&id)?;
        }

        transaction.commit().unwrap();

        Ok(())
    }

    fn recreate_conflicting_file(&mut self, id: &String) -> Result<()> {
        self.store.inner.remove_file_from_journal(&id)?;
        let new_id = self.store.inner.assign_temp_id(&id)?;

        let dirent = self.store.query_file(&new_id)?;
        let parent_dirent = self.store.query_file(&dirent.parent)?;

        let recreate_file_op = ModifyOpBuilder::make_recreate_file_op(&parent_dirent, &dirent);
        let recreate_file_op_proto: proto_types::ModifyOperation = recreate_file_op.into();
        self.store
            .inner
            .add_journal_entry(&dirent.parent, &recreate_file_op_proto.encode_to_vec())?;

        let reset_attributes_op = ModifyOpBuilder::make_reset_attributes_op(&dirent);
        let reset_attributes_op_proto: proto_types::ModifyOperation = reset_attributes_op.into();
        self.store
            .inner
            .add_journal_entry(&new_id, &reset_attributes_op_proto.encode_to_vec())?;

        Ok(())
    }
}
