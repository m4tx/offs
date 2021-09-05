use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use digest::Digest;
use rusqlite::types::Null;
use rusqlite::{params, params_from_iter, Connection, Row, ToSql};
use sha2::Sha256;

use crate::store::id_generator::{LocalTempIdGenerator, RandomHexIdGenerator};
use crate::{ROOT_ID, SQLITE_CACHE_SIZE, SQLITE_PAGE_SIZE};

use self::id_generator::IdGenerator;
pub use self::types::{DirEntity, FileDev, FileMode, FileStat, FileType};
use crate::errors::OperationResult;
use crate::timespec::Timespec;

pub mod id_generator;
mod types;
pub mod wrapper;

pub struct Store<T: IdGenerator> {
    connection: Arc<Mutex<Connection>>,
    db_path: PathBuf,

    id_generator: T,
}

impl Store<RandomHexIdGenerator> {
    pub fn new_with_random_id_generator(db_path: impl AsRef<std::path::Path>) -> Self {
        Self::new(db_path, RandomHexIdGenerator::new())
    }

    pub fn new_server(db_path: impl AsRef<std::path::Path>) -> Self {
        Self::new_with_random_id_generator(db_path)
    }

    pub fn increment_dirent_version(&mut self, id: &str) {
        self.connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE file SET dirent_version = dirent_version + 1 WHERE id = ?",
                params![id],
            )
            .unwrap();
    }

    pub fn increment_content_version(&mut self, id: &str) {
        self.connection
            .lock()
            .unwrap()
            .execute(
                r#"
                UPDATE file
                SET dirent_version  = dirent_version + 1,
                    content_version = content_version + 1
                WHERE id = ?"#,
                params![id],
            )
            .unwrap();
    }
}

impl Store<LocalTempIdGenerator> {
    pub fn new_with_local_temp_id_generator(db_path: impl AsRef<std::path::Path>) -> Self {
        Self::new(db_path, LocalTempIdGenerator::new())
    }

    pub fn new_client(db_path: impl AsRef<std::path::Path>) -> Self {
        let mut store = Self::new_with_local_temp_id_generator(db_path);

        store
            .connection
            .lock()
            .unwrap()
            .execute_batch(include_str!("sql/init_client.sql"))
            .unwrap();
        let next_id = store.get_next_temp_id();
        store.id_generator.next_id.store(next_id, Ordering::Relaxed);

        store
    }

    fn get_next_temp_id(&mut self) -> usize {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare("SELECT id FROM file WHERE id LIKE 'temp-%' ORDER BY id DESC LIMIT 1")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();

        if let Some(row) = rows.next().unwrap() {
            LocalTempIdGenerator::get_n(&row.get::<_, String>(0).unwrap()) + 1
        } else {
            0
        }
    }

    pub fn get_temp_chunks(&mut self) -> Vec<String> {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare(
                r#"
                SELECT DISTINCT blob
                FROM file
                         JOIN chunk fb on file.id = fb.file
                WHERE id LIKE "temp-%""#,
            )
            .unwrap();

        let iter = stmt.query_map([], |row| Ok(row.get(0)?)).unwrap();

        iter.map(|x| x.unwrap()).collect()
    }

    pub fn get_temp_file_ids(&self) -> impl Iterator<Item = String> {
        let val = self.id_generator.next_id.load(Ordering::Relaxed);

        (0..val).map(|x| LocalTempIdGenerator::get_nth_id(x))
    }

    pub fn add_journal_entry(&self, id: &str, operation: &[u8]) -> i64 {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare("INSERT INTO journal (file, operation) VALUES (?, ?)")
            .unwrap();

        stmt.insert(params![id, operation]).unwrap()
    }

    pub fn get_journal(&self) -> Vec<Vec<u8>> {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection.prepare("SELECT operation FROM journal").unwrap();
        let iter = stmt.query_map([], |row| Ok(row.get(0).unwrap())).unwrap();

        iter.map(|x| x.unwrap()).collect()
    }

    pub fn clear_journal(&mut self) {
        self.connection
            .lock()
            .unwrap()
            .execute("DELETE FROM journal", [])
            .unwrap();
        self.id_generator.reset_generator();
    }

    pub fn remove_file_from_journal(&self, id: &str) {
        self.connection
            .lock()
            .unwrap()
            .execute("DELETE FROM journal WHERE file = ?", params![id])
            .unwrap();
    }

    pub fn remove_journal_item(&self, id: i64) {
        self.connection
            .lock()
            .unwrap()
            .execute("DELETE FROM journal WHERE id = ?", params![id])
            .unwrap();
    }

    pub fn assign_temp_id(&mut self, id: &str) -> String {
        let new_id = self.id_generator.generate_id();
        self.change_id(id, &new_id);

        new_id
    }

    pub fn update_retrieved_version(&self, id: &str) {
        self.connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE file SET retrieved_version = content_version WHERE id = ?",
                params![id],
            )
            .unwrap();
    }

    pub fn remove_remaining_files<T: IntoIterator>(&self, parent_id: &str, to_keep: T)
    where
        T::Item: AsRef<str>,
        T::IntoIter: ExactSizeIterator,
    {
        let iter = to_keep.into_iter();

        if iter.len() == 0 {
            return;
        }

        let args_str = itertools::join((0..iter.len()).into_iter().map(|_x| "?"), ", ");
        let query = format!(
            "DELETE FROM file WHERE parent = ? AND id NOT IN ({})",
            args_str
        );

        let connection = self.connection.lock().unwrap();
        let mut stmt = connection.prepare(&query).unwrap();
        let params =
            std::iter::once(parent_id.to_owned()).chain(iter.map(|x| x.as_ref().to_owned()));

        stmt.execute(params_from_iter(params)).unwrap();
    }
}

impl<IdT: IdGenerator> Store<IdT> {
    pub fn new(db_path: impl AsRef<std::path::Path>, id_generator: IdT) -> Self {
        let cloned_db_path = db_path.as_ref().to_owned();
        let connection = Self::create_connection(db_path);

        connection
            .execute_batch(include_str!("sql/init.sql"))
            .unwrap();

        let store = Self {
            connection: Arc::new(Mutex::new(connection)),
            db_path: cloned_db_path,

            id_generator,
        };

        store.run_gc();

        store
    }

    fn create_connection(db_path: impl AsRef<std::path::Path>) -> Connection {
        let connection = Connection::open(db_path).unwrap();

        connection
            .pragma_update(None, "foreign_keys", &true)
            .unwrap();
        connection
            .pragma_update(None, "page_size", &SQLITE_PAGE_SIZE)
            .unwrap();
        connection
            .pragma_update(None, "cache_size", &SQLITE_CACHE_SIZE)
            .unwrap();
        connection
            .pragma_update(None, "journal_mode", &"WAL")
            .unwrap();

        connection
    }

    pub fn reset_id_generator(&mut self) {
        self.id_generator.reset_generator();
    }

    fn convert_file_data(row: &Row) -> rusqlite::Result<DirEntity> {
        let parent = row.get::<_, Option<String>>("parent")?;

        Ok(DirEntity {
            id: row.get("id")?,
            parent: parent.unwrap_or("".to_owned()),
            name: row.get("name")?,

            dirent_version: row.get("dirent_version")?,
            content_version: row.get("content_version")?,
            retrieved_version: row.get("retrieved_version")?,

            stat: FileStat {
                file_type: num_traits::FromPrimitive::from_i64(row.get("file_type")?).unwrap(),
                mode: row.get("mode")?,
                dev: row.get("dev")?,
                nlink: 2,
                uid: 1000,
                gid: 1000,
                size: row.get::<_, i64>("size")? as u64,
                blocks: 1,
                atim: Timespec::new(row.get("atim")?, row.get("atimns")?),
                mtim: Timespec::new(row.get("mtim")?, row.get("mtimns")?),
                ctim: Timespec::new(row.get("ctim")?, row.get("ctimns")?),
            },
        })
    }

    pub fn list_files(&self, parent_id: &str) -> Vec<DirEntity> {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare("SELECT * FROM file WHERE parent = ?")
            .unwrap();
        let iter = stmt
            .query_map(params![parent_id], Self::convert_file_data)
            .unwrap();

        iter.map(|x| x.unwrap()).collect()
    }

    pub fn file_exists(&self, id: &str) -> bool {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare("SELECT 1 FROM file WHERE id = ?")
            .unwrap();
        stmt.exists(params![id]).unwrap()
    }

    pub fn any_child_exists(&self, id: &str) -> bool {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare("SELECT 1 FROM file WHERE parent = ?")
            .unwrap();
        stmt.exists(params![id]).unwrap()
    }

    pub fn file_exists_by_name(&self, parent_id: &str, name: &str) -> bool {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare("SELECT 1 FROM file WHERE parent = ? AND name = ?")
            .unwrap();
        stmt.exists(params![parent_id, name]).unwrap()
    }

    pub fn query_file(&self, id: &str) -> Option<DirEntity> {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare("SELECT * FROM file WHERE id = ?")
            .unwrap();
        let mut rows = stmt.query(params![id]).unwrap();

        if let Some(row) = rows.next().unwrap() {
            Some(Self::convert_file_data(row).unwrap())
        } else {
            None
        }
    }

    pub fn query_file_by_name(&self, parent_id: &str, name: &str) -> Option<DirEntity> {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare("SELECT * FROM file WHERE parent = ? AND name = ?")
            .unwrap();
        let mut rows = stmt.query(params![parent_id, name]).unwrap();

        if let Some(row) = rows.next().unwrap() {
            Some(Self::convert_file_data(row).unwrap())
        } else {
            None
        }
    }

    pub fn resize_file(&self, id: &str, size: u64) {
        self.connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE file SET size = ? WHERE id = ?",
                params![size as i64, id],
            )
            .unwrap();
    }

    pub fn create_file(
        &mut self,
        parent_id: &str,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
        creation_time: Timespec,
    ) -> String {
        self.create_file_entity(parent_id, None, name, file_type, mode, dev, creation_time)
    }

    pub fn create_directory(
        &mut self,
        parent_id: &str,
        name: &str,
        mode: FileMode,
        creation_time: Timespec,
    ) -> String {
        self.create_file_entity(
            parent_id,
            None,
            name,
            FileType::Directory,
            mode,
            0,
            creation_time,
        )
    }

    pub fn add_or_replace_item(&self, dirent: &DirEntity) {
        let parent = if dirent.id == ROOT_ID {
            &Null as &dyn ToSql
        } else {
            &dirent.parent as &dyn ToSql
        };

        self.connection
            .lock()
            .unwrap()
            .execute(
                r#"INSERT OR IGNORE INTO file (
                 id, parent, name, dirent_version, content_version,
                 file_type, mode, dev, size, atim, atimns, mtim, mtimns, ctim, ctimns
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
                params![
                    dirent.id,
                    parent,
                    dirent.name,
                    dirent.dirent_version,
                    dirent.content_version,
                    dirent.stat.file_type as i64,
                    dirent.stat.mode,
                    dirent.stat.dev,
                    dirent.stat.size as i64,
                    dirent.stat.atim.sec,
                    dirent.stat.atim.nsec,
                    dirent.stat.mtim.sec,
                    dirent.stat.mtim.nsec,
                    dirent.stat.ctim.sec,
                    dirent.stat.ctim.nsec,
                ],
            )
            .unwrap();
        self.connection
            .lock()
            .unwrap()
            .execute(
                r#"
                UPDATE file
                SET parent          = ?,
                    name            = ?,
                    dirent_version  = ?,
                    content_version = ?,
                    file_type       = ?,
                    mode            = ?,
                    dev             = ?,
                    size            = ?,
                    atim            = ?,
                    atimns          = ?,
                    mtim            = ?,
                    mtimns          = ?,
                    ctim            = ?,
                    ctimns          = ?
                WHERE id = ?"#,
                params![
                    parent,
                    dirent.name,
                    dirent.dirent_version,
                    dirent.content_version,
                    dirent.stat.file_type as i64,
                    dirent.stat.mode,
                    dirent.stat.dev,
                    dirent.stat.size as i64,
                    dirent.stat.atim.sec,
                    dirent.stat.atim.nsec,
                    dirent.stat.mtim.sec,
                    dirent.stat.mtim.nsec,
                    dirent.stat.ctim.sec,
                    dirent.stat.ctim.nsec,
                    dirent.id,
                ],
            )
            .unwrap();
    }

    pub fn create_default_root_directory(&mut self) {
        self.create_root_directory(0o755, Timespec { sec: 0, nsec: 0 });
    }

    pub fn create_root_directory(&mut self, mode: FileMode, creation_time: Timespec) {
        if !self.file_exists(ROOT_ID) {
            self.create_file_entity(
                "",
                Some(ROOT_ID),
                "",
                FileType::Directory,
                mode,
                0,
                creation_time,
            );
        }
    }

    fn create_file_entity(
        &mut self,
        parent_id: &str,
        id: Option<&str>,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
        creation_time: Timespec,
    ) -> String {
        let id = match id {
            Some(x) => x.to_owned(),
            None => self.id_generator.generate_id(),
        };

        self.connection
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO file (\
                 id, parent, name, dirent_version, content_version,\
                 file_type, mode, dev, size, atim, atimns, mtim, mtimns, ctim, ctimns\
                 ) VALUES (?, ?, ?, 1, 1, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)",
                params![
                    id,
                    if id == ROOT_ID {
                        &Null as &dyn ToSql
                    } else {
                        &parent_id as &dyn ToSql
                    },
                    name,
                    file_type as i64,
                    mode,
                    dev as i64,
                    creation_time.sec,
                    creation_time.nsec,
                    creation_time.sec,
                    creation_time.nsec,
                    creation_time.sec,
                    creation_time.nsec,
                ],
            )
            .unwrap();

        id
    }

    pub fn remove_file(&self, id: &str) {
        self.connection
            .lock()
            .unwrap()
            .execute("DELETE FROM file WHERE id = ?", params![id])
            .unwrap();
    }

    pub fn remove_directory(&self, id: &str) -> OperationResult<()> {
        self.connection
            .lock()
            .unwrap()
            .execute("DELETE FROM file WHERE id = ?", params![id])
            .unwrap();

        Ok(())
    }

    pub fn get_chunks(&self, id: &str) -> Vec<String> {
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection
            .prepare(r#"SELECT blob FROM chunk WHERE file = ? ORDER BY "index""#)
            .unwrap();

        let iter = stmt.query_map(params![id], |row| Ok(row.get(0)?)).unwrap();

        iter.map(|x| x.unwrap()).collect()
    }

    pub fn get_blobs<T: IntoIterator>(&self, ids: T) -> HashMap<String, Vec<u8>>
    where
        T::Item: AsRef<str>,
        T::IntoIter: ExactSizeIterator,
    {
        let iter = ids.into_iter();
        let mut map = HashMap::with_capacity(iter.len());

        if iter.len() == 0 {
            return map;
        }

        let args_str = itertools::join((0..iter.len()).into_iter().map(|_x| "?"), ", ");
        let query = "SELECT * FROM blob WHERE id IN (".to_owned() + &args_str + ")";
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection.prepare(&query).unwrap();
        let params = iter.map(|x| x.as_ref().to_owned());
        let mut rows = stmt.query(params_from_iter(params)).unwrap();

        while let Some(row) = rows.next().unwrap() {
            map.insert(row.get(0).unwrap(), row.get(1).unwrap());
        }

        map
    }

    pub fn get_blob(&self, id: impl AsRef<str>) -> Vec<u8> {
        self.get_blobs([id.as_ref()].iter())
            .remove(id.as_ref())
            .unwrap()
    }

    pub fn get_missing_blobs<T: IntoIterator>(&self, ids: T) -> Vec<String>
    where
        T::Item: AsRef<str>,
        T::IntoIter: ExactSizeIterator,
    {
        let ids_iter = ids.into_iter();
        let ids_len = ids_iter.len();

        if ids_len == 0 {
            return Vec::new();
        }

        let args_str = itertools::join(
            (0..ids_len).into_iter().map(|_| "SELECT ? AS id"),
            " UNION ",
        );
        let query = format!(
            r#"SELECT t.id FROM ({}) AS t LEFT JOIN blob ON t.id = blob.id WHERE blob.id IS NULL;"#,
            args_str
        );
        let connection = self.connection.lock().unwrap();
        let mut stmt = connection.prepare(&query).unwrap();

        let params = ids_iter.map(|x| x.as_ref().to_owned());
        let rows = stmt
            .query_map(params_from_iter(params), |row| Ok(row.get(0)?))
            .unwrap();

        rows.map(|x| x.unwrap()).collect()
    }

    fn get_blob_id(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    pub fn add_blob(&self, data: &[u8]) -> String {
        let mut length = data.len();
        while length >= 1 && data[length - 1] == 0u8 {
            length -= 1;
        }
        let data = &data[..length];

        let id = Self::get_blob_id(data);

        self.connection
            .lock()
            .unwrap()
            .execute(
                "INSERT OR IGNORE INTO blob (id, content) VALUES (?, ?)",
                params![id, data],
            )
            .unwrap();

        id
    }

    pub fn add_blobs(&self, blobs: impl IntoIterator<Item = impl AsRef<[u8]>>) {
        for blob in blobs {
            self.add_blob(blob.as_ref());
        }
    }

    pub fn replace_chunks<T: AsRef<str>>(
        &self,
        id: &str,
        chunks: impl IntoIterator<Item = (usize, T)>,
    ) {
        for (index, blob_id) in chunks.into_iter() {
            self.replace_chunk(id, index, blob_id.as_ref());
        }
    }

    pub fn replace_chunk(&self, id: &str, index: usize, blob_id: &str) {
        self.connection
            .lock()
            .unwrap()
            .execute(
                r#"INSERT OR REPLACE INTO chunk (file, blob, "index") VALUES (?, ?, ?)"#,
                params![id, blob_id, index as i64],
            )
            .unwrap();
    }

    pub fn truncate_chunks(&self, id: &str, remove_since_id: usize) {
        self.connection
            .lock()
            .unwrap()
            .execute(
                r#"DELETE FROM chunk WHERE file = ? AND "index" >= ?"#,
                params![id, remove_since_id as i64],
            )
            .unwrap();
    }

    pub fn transaction(&self) -> Transaction {
        Transaction::new(self.connection.clone())
    }

    pub fn set_attributes(
        &self,
        id: &str,
        mode: Option<FileMode>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        atim: Option<Timespec>,
        mtim: Option<Timespec>,
        ctim: Option<Timespec>,
    ) {
        let mut columns = Vec::new();
        let mut values: Vec<&dyn ToSql> = Vec::new();

        let mode_val = mode.unwrap_or(Default::default());
        if mode.is_some() {
            columns.push("mode");
            values.push(&mode_val);
        }

        let size_val = size.unwrap_or(Default::default()) as i64;
        if size.is_some() {
            columns.push("size");
            values.push(&size_val);
        }

        let atim_val = atim.unwrap_or(Timespec::new(0, 0));
        if atim.is_some() {
            columns.push("atim");
            values.push(&atim_val.sec);
            columns.push("atimns");
            values.push(&atim_val.nsec);
        }

        let mtim_val = mtim.unwrap_or(Timespec::new(0, 0));
        if mtim.is_some() {
            columns.push("mtim");
            values.push(&mtim_val.sec);
            columns.push("mtimns");
            values.push(&mtim_val.nsec);
        }

        let ctim_val = ctim.unwrap_or(Timespec::new(0, 0));
        if ctim.is_some() {
            columns.push("ctim");
            values.push(&ctim_val.sec);
            columns.push("ctimns");
            values.push(&ctim_val.nsec);
        }

        values.push(&id);

        if columns.is_empty() {
            return;
        }

        let args_str = itertools::join(columns.iter().map(|x| format!("{} = ?", x)), ", ");
        let query = format!("UPDATE file SET {} WHERE id = ?", args_str);

        self.connection
            .lock()
            .unwrap()
            .execute(&query, params_from_iter(values))
            .unwrap();
    }

    pub fn rename(&self, id: &str, new_parent: &str, new_name: &str) {
        self.connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE file SET parent = ?, name = ? WHERE id = ?",
                params![new_parent, new_name, id],
            )
            .unwrap();
    }

    pub fn change_id(&self, old_id: &str, new_id: &str) {
        let connection = self.connection.lock().unwrap();
        connection
            .execute(
                "UPDATE file SET id = ? WHERE id = ?",
                params![new_id, old_id],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE chunk SET file = ? WHERE file = ?",
                params![new_id, old_id],
            )
            .unwrap();
    }

    pub fn run_gc(&self) {
        self.connection
            .lock()
            .unwrap()
            .execute(
                r#"
                DELETE
                FROM blob
                WHERE id IN (
                    SELECT DISTINCT id
                    FROM blob
                             LEFT JOIN chunk ON blob.id = chunk.blob
                    WHERE chunk.blob IS NULL
                )"#,
                [],
            )
            .unwrap();
    }
}

impl<IdT: IdGenerator> Clone for Store<IdT> {
    fn clone(&self) -> Self {
        return Self {
            connection: Arc::new(Mutex::new(Self::create_connection(&self.db_path))),
            db_path: self.db_path.clone(),

            id_generator: self.id_generator.clone(),
        };
    }
}

pub struct Transaction {
    connection: Arc<Mutex<Connection>>,
    committed: bool,
}

impl Transaction {
    fn new(connection: Arc<Mutex<Connection>>) -> Self {
        connection
            .lock()
            .unwrap()
            .execute("BEGIN", [])
            .expect("Cannot start transaction");

        Self {
            connection,
            committed: false,
        }
    }

    pub fn commit(mut self) -> Result<usize, rusqlite::Error> {
        self.committed = true;
        self.connection.lock().unwrap().execute("COMMIT", [])
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            self.connection
                .lock()
                .unwrap()
                .execute("ROLLBACK", [])
                .expect("Could not rollback transaction");
        }
    }
}
