use std::cmp::{max, min};
use std::collections::HashMap;
use std::iter;

use crate::errors::{OperationError, OperationResult};
use crate::store::id_generator::{IdGenerator, LocalTempIdGenerator, RandomHexIdGenerator};
use crate::store::{DirEntity, FileDev, FileMode, FileType, Store, Transaction};
use crate::timespec::Timespec;
use crate::BLOB_SIZE;

#[derive(Clone)]
pub struct StoreWrapper<T: IdGenerator> {
    inner: Store<T>,
}

impl<IdT: IdGenerator> StoreWrapper<IdT> {
    pub fn new(store: Store<IdT>) -> Self {
        Self { inner: store }
    }

    // Read
    pub fn try_query_file(&self, id: &str) -> OperationResult<Option<DirEntity>> {
        self.inner.query_file(id)
    }

    pub fn query_file(&self, id: &str) -> OperationResult<DirEntity> {
        self.inner
            .query_file(id)?
            .ok_or(OperationError::file_does_not_exist(&format!("id={}", id)))
    }

    pub fn try_query_file_by_name(
        &self,
        parent_id: &str,
        name: &str,
    ) -> OperationResult<Option<DirEntity>> {
        self.inner.query_file_by_name(parent_id, name)
    }

    pub fn query_file_by_name(&self, parent_id: &str, name: &str) -> OperationResult<DirEntity> {
        self.inner
            .query_file_by_name(parent_id, name)?
            .ok_or(OperationError::file_does_not_exist(&format!(
                "parent={}, name={}",
                parent_id, name
            )))
    }

    pub fn file_exists_by_name(&self, parent_id: &str, name: &str) -> OperationResult<bool> {
        Ok(self.inner.file_exists_by_name(parent_id, name)?)
    }

    pub fn any_child_exists(&self, id: &str) -> OperationResult<bool> {
        Ok(self.inner.any_child_exists(id)?)
    }

    pub fn list_files(&self, parent_id: &str) -> OperationResult<Vec<DirEntity>> {
        Ok(self.inner.list_files(parent_id)?)
    }

    fn get_start_end_chunks(offset: i64, size: u32, chunk_num: usize) -> (usize, usize) {
        const IBLOB_SIZE: i64 = BLOB_SIZE as i64;

        let start_blob = (offset / IBLOB_SIZE) as usize;
        let end_blob = min(
            chunk_num,
            ((offset + size as i64) / IBLOB_SIZE + 1) as usize,
        );

        (start_blob, end_blob)
    }

    fn get_data(
        &self,
        chunks: &Vec<String>,
        blobs: &HashMap<String, Vec<u8>>,
        offset: i64,
        size: u32,
    ) -> Vec<u8> {
        if chunks.is_empty() {
            return Vec::new();
        }

        let mut vec = Vec::with_capacity(size as usize);
        let start_index = (offset % BLOB_SIZE as i64) as usize;

        let first_chunk = blobs[&chunks[0]]
            .iter()
            .skip(start_index)
            .take(size as usize);
        vec.extend(first_chunk);

        for i in 1..chunks.len() {
            let chunk_length = size as usize - vec.len();
            let chunk = &blobs[&chunks[i]];
            let chunk_iter = chunk.iter().take(chunk_length);
            vec.extend(chunk_iter);

            let real_chunk_length = min(chunk_length, BLOB_SIZE);
            if chunk.len() < real_chunk_length {
                vec.extend(iter::repeat(0u8).take(real_chunk_length - chunk.len()));
            }
        }

        vec
    }

    pub fn get_blobs<T: IntoIterator>(&self, ids: T) -> OperationResult<HashMap<String, Vec<u8>>>
    where
        T::Item: AsRef<str>,
        T::IntoIter: ExactSizeIterator,
    {
        Ok(self.inner.get_blobs(ids)?)
    }

    pub fn get_missing_blobs<T: IntoIterator>(&self, ids: T) -> OperationResult<Vec<String>>
    where
        T::Item: AsRef<str>,
        T::IntoIter: ExactSizeIterator,
    {
        Ok(self.inner.get_missing_blobs(ids)?)
    }

    pub fn get_chunks(&self, id: &str) -> OperationResult<Vec<String>> {
        Ok(self.inner.get_chunks(id)?)
    }

    pub fn add_blobs(
        &self,
        blobs: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> OperationResult<()> {
        Ok(self.inner.add_blobs(blobs)?)
    }

    fn get_blobs_for_read(
        &mut self,
        id: &str,
        offset: i64,
        size: u32,
    ) -> OperationResult<Vec<String>> {
        let chunks = self.inner.get_chunks(id)?;
        let (start_chunk, end_chunk) = Self::get_start_end_chunks(offset, size, chunks.len());

        let result = if start_chunk >= chunks.len() {
            Vec::new()
        } else {
            chunks[start_chunk..end_chunk].to_vec()
        };

        Ok(result)
    }

    pub fn get_missing_blobs_for_read(
        &mut self,
        id: &str,
        offset: i64,
        size: u32,
    ) -> OperationResult<Vec<String>> {
        let chunks = self.get_blobs_for_read(id, offset, size)?;
        Ok(self.inner.get_missing_blobs(&chunks)?)
    }

    pub fn read(&mut self, id: &str, offset: i64, size: u32) -> OperationResult<Vec<u8>> {
        let chunks = self.get_blobs_for_read(id, offset, size)?;
        let blobs = self.inner.get_blobs(chunks.iter())?;

        Ok(self.get_data(&chunks, &blobs, offset, size))
    }

    pub fn update_time(
        &mut self,
        id: &str,
        timestamp: Timespec,
        update_atime: bool,
        update_mtime: bool,
        update_ctime: bool,
    ) -> OperationResult<()> {
        let atime = if update_atime { Some(timestamp) } else { None };
        let mtime = if update_mtime { Some(timestamp) } else { None };
        let ctime = if update_ctime { Some(timestamp) } else { None };

        self.inner
            .set_attributes(id, None, None, None, None, atime, mtime, ctime)?;

        Ok(())
    }

    // Create
    pub fn create_default_root_directory(&mut self) -> OperationResult<()> {
        Ok(self.inner.create_default_root_directory()?)
    }

    pub fn add_or_replace_dirent(&self, dirent: &DirEntity) -> OperationResult<()> {
        Ok(self.inner.add_or_replace_dirent(dirent)?)
    }

    pub fn create_file(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> OperationResult<String> {
        let id = self
            .inner
            .create_file(parent_id, name, file_type, mode, dev, timestamp)?;

        self.update_time(parent_id, timestamp, false, true, true)?;

        Ok(id)
    }

    pub fn create_directory(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        mode: FileMode,
    ) -> OperationResult<String> {
        let id = self
            .inner
            .create_directory(parent_id, name, mode, timestamp)?;

        self.update_time(parent_id, timestamp, false, true, true)?;

        Ok(id)
    }

    pub fn create_symlink(
        &mut self,
        parent_id: &str,
        timestamp: Timespec,
        name: &str,
        link: &str,
    ) -> OperationResult<String> {
        let id = self.create_file(parent_id, timestamp, name, FileType::Symlink, 0o777, 0)?;

        self.write(&id, timestamp, 0, link.as_bytes())?;

        Ok(id)
    }

    // Remove
    pub fn remove_file(&mut self, id: &str, timestamp: Timespec) -> OperationResult<()> {
        let dirent = self.query_file(id)?;

        self.inner.remove_file(id)?;

        self.update_time(&dirent.parent, timestamp, false, true, true)?;

        Ok(())
    }

    pub fn remove_directory(&mut self, id: &str, timestamp: Timespec) -> OperationResult<()> {
        let dirent = self.query_file(id)?;

        self.inner.remove_directory(id)?;

        self.update_time(&dirent.parent, timestamp, false, true, true)?;

        Ok(())
    }

    // Modify
    pub fn change_id(&self, old_id: &str, new_id: &str) -> OperationResult<()> {
        Ok(self.inner.change_id(old_id, new_id)?)
    }

    pub fn rename(
        &mut self,
        id: &str,
        timestamp: Timespec,
        new_parent: &str,
        new_name: &str,
    ) -> OperationResult<()> {
        let dirent = self.query_file(id)?;

        self.inner.rename(id, new_parent, new_name)?;

        self.update_time(&dirent.parent, timestamp, false, true, true)?;
        self.update_time(new_parent, timestamp, false, true, true)?;
        self.update_time(id, timestamp, false, false, true)?;

        Ok(())
    }

    pub fn resize_file(&mut self, id: &str, new_size: u64) -> OperationResult<()> {
        let dirent = self.query_file(id)?;

        let old_size = dirent.stat.size;
        let old_chunk_count = (old_size as usize + BLOB_SIZE - 1) / BLOB_SIZE;
        let new_chunk_count = (new_size as usize + BLOB_SIZE - 1) / BLOB_SIZE;
        let chunks = self.inner.get_chunks(id)?;

        // Adjust the chunks
        if new_chunk_count > old_chunk_count {
            let zero_blob = self.inner.add_blob(&[])?;
            let iter = std::iter::repeat(&zero_blob)
                .enumerate()
                .skip(old_chunk_count)
                .take(new_chunk_count - old_chunk_count);
            self.inner.replace_chunks(id, iter)?;
        } else if new_chunk_count < old_chunk_count {
            self.inner.truncate_chunks(id, new_chunk_count)?;
        }

        // Adjust the last chunk
        let last_chunk_size = (new_size % BLOB_SIZE as u64) as usize;

        if last_chunk_size != 0 && new_size < old_size {
            let last_chunk_index = new_chunk_count - 1;

            let mut last_chunk = self.inner.get_blob(&chunks[last_chunk_index])?;
            if last_chunk_size < last_chunk.len() {
                last_chunk.resize(last_chunk_size, 0);

                let last_chunk_blob = self.inner.add_blob(&last_chunk)?;
                self.inner
                    .replace_chunk(id, last_chunk_index, &last_chunk_blob)?;
            }
        }

        Ok(())
    }

    pub fn set_attributes(
        &mut self,
        id: &str,
        timestamp: Timespec,
        mode: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atim: Option<Timespec>,
        mut mtim: Option<Timespec>,
    ) -> OperationResult<()> {
        if let Some(size_val) = size {
            self.resize_file(id, size_val)?;
            mtim = Some(timestamp);
        }

        let ctim = if mode.is_some() || uid.is_some() || gid.is_some() || size.is_some() {
            Some(timestamp)
        } else {
            None
        };

        self.inner
            .set_attributes(id, mode, uid, gid, size, atim, mtim, ctim)?;

        Ok(())
    }

    pub fn write(
        &mut self,
        id: &str,
        timestamp: Timespec,
        offset: usize,
        data: &[u8],
    ) -> OperationResult<()> {
        let chunks = self.inner.get_chunks(id)?;
        let mut blobs = self.inner.get_blobs(chunks.iter())?;
        let mut new_chunks = Vec::new();
        let mut data_offset: usize = 0;
        let first_chunk_id = offset / BLOB_SIZE;

        {
            // The first chunk
            let chunk_offset = offset % BLOB_SIZE;
            let first_chunk_size = min(data.len(), BLOB_SIZE - chunk_offset);
            let mut chunk = blobs
                .remove(chunks.get(first_chunk_id).unwrap_or(&"".to_owned()))
                .unwrap_or(Vec::new());

            chunk.resize(BLOB_SIZE, 0);
            chunk.as_mut_slice()[chunk_offset..chunk_offset + first_chunk_size]
                .copy_from_slice(&data[..first_chunk_size]);

            new_chunks.push(self.inner.add_blob(&chunk)?);

            data_offset += first_chunk_size;
        }

        // Middle chunks
        while data_offset + BLOB_SIZE <= data.len() {
            new_chunks.push(
                self.inner
                    .add_blob(&data[data_offset..data_offset + BLOB_SIZE])?,
            );

            data_offset += BLOB_SIZE;
        }

        // The last chunk
        if data_offset < data.len() {
            let last_chunk_size = data.len() - data_offset;

            let mut chunk = blobs
                .remove(&chunks[(offset + data_offset) / BLOB_SIZE])
                .unwrap_or(Vec::new());
            chunk.resize(BLOB_SIZE, 0);

            chunk.as_mut_slice()[..last_chunk_size].copy_from_slice(&data[data_offset..]);

            new_chunks.push(self.inner.add_blob(&chunk)?);
        }

        // Update the store
        self.inner.replace_chunks(
            id,
            new_chunks
                .iter()
                .enumerate()
                .map(|(i, value)| (i + first_chunk_id, value)),
        )?;
        let dirent = self.query_file(id)?;
        self.inner
            .resize_file(id, max(dirent.stat.size, (offset + data.len()) as u64))?;

        self.update_time(id, timestamp, false, true, true)?;

        Ok(())
    }

    pub fn add_blob(&self, data: &[u8]) -> OperationResult<String> {
        Ok(self.inner.add_blob(data)?)
    }

    pub fn replace_chunks<T: AsRef<str>>(
        &self,
        id: &str,
        chunks: impl IntoIterator<Item = (usize, T)>,
    ) -> OperationResult<()> {
        Ok(self.inner.replace_chunks(id, chunks)?)
    }

    // Misc
    pub fn transaction(&self) -> Transaction {
        self.inner.transaction()
    }
}

impl StoreWrapper<RandomHexIdGenerator> {
    // Modify
    pub fn increment_dirent_version(&mut self, id: &str) -> OperationResult<()> {
        Ok(self.inner.increment_dirent_version(id)?)
    }

    pub fn increment_content_version(&mut self, id: &str) -> OperationResult<()> {
        Ok(self.inner.increment_content_version(id)?)
    }
}

impl StoreWrapper<LocalTempIdGenerator> {
    // Read
    pub fn get_temp_chunks(&mut self) -> OperationResult<Vec<String>> {
        Ok(self.inner.get_temp_chunks()?)
    }

    pub fn get_temp_file_ids(&self) -> impl Iterator<Item = String> {
        self.inner.get_temp_file_ids()
    }

    // Modify
    pub fn update_retrieved_version(&self, id: &str) -> OperationResult<()> {
        Ok(self.inner.update_retrieved_version(id)?)
    }

    pub fn remove_remaining_files<T: IntoIterator>(
        &self,
        parent_id: &str,
        to_keep: T,
    ) -> OperationResult<()>
    where
        T::Item: AsRef<str>,
        T::IntoIter: ExactSizeIterator,
    {
        Ok(self.inner.remove_remaining_files(parent_id, to_keep)?)
    }

    pub fn assign_temp_id(&mut self, id: &str) -> OperationResult<String> {
        Ok(self.inner.assign_temp_id(id)?)
    }

    // Journal
    pub fn add_journal_entry(&self, id: &str, operation: &[u8]) -> OperationResult<i64> {
        Ok(self.inner.add_journal_entry(id, operation)?)
    }

    pub fn get_journal(&self) -> OperationResult<Vec<Vec<u8>>> {
        Ok(self.inner.get_journal()?)
    }

    pub fn clear_journal(&mut self) -> OperationResult<()> {
        Ok(self.inner.clear_journal()?)
    }

    pub fn remove_file_from_journal(&self, id: &str) -> OperationResult<()> {
        Ok(self.inner.remove_file_from_journal(id)?)
    }

    pub fn remove_journal_item(&self, id: i64) -> OperationResult<()> {
        Ok(self.inner.remove_journal_item(id)?)
    }
}
