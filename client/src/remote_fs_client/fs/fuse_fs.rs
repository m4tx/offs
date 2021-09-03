use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{
    FileAttr, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite, Request, TimeOrNow,
};
use libc::{S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFREG, S_IFSOCK};
use log::debug;
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, RwLock};

use offs::store::{DirEntity, FileMode, FileType};
use offs::timespec::Timespec;

use super::error::{RemoteFsError, RemoteFsErrorKind};
use super::OffsFilesystem;
use super::Result;
use offs::ROOT_ID;
use std::cell::RefCell;
use std::collections::HashMap;

const TTL: Duration = Duration::from_secs(1);

macro_rules! try_fs {
    ($e:expr, $reply:ident) => {
        match $e {
            Ok(val) => val,
            Err(e) => {
                $reply.error(e.to_os_error());
                debug!("Response: {:?}", e);
                return;
            }
        }
    };
}

struct FuseHelper {
    next_inode: RefCell<u64>,
    inodes_to_ids: RefCell<HashMap<u64, String>>,
    ids_to_inodes: RefCell<HashMap<String, u64>>,
}

impl FuseHelper {
    fn new() -> Self {
        Self {
            next_inode: RefCell::new(2),
            inodes_to_ids: RefCell::new([(1, ROOT_ID.to_owned())].iter().cloned().collect()),
            ids_to_inodes: RefCell::new([(ROOT_ID.to_owned(), 1)].iter().cloned().collect()),
        }
    }

    fn get_inode_for_id(&self, id: &str) -> u64 {
        if !self.ids_to_inodes.borrow().contains_key(id) {
            let next_inode_val = *self.next_inode.borrow();
            self.ids_to_inodes
                .borrow_mut()
                .insert(id.to_owned(), next_inode_val);
            self.inodes_to_ids
                .borrow_mut()
                .insert(next_inode_val, id.to_owned());

            *self.next_inode.borrow_mut() += 1;
        };

        self.ids_to_inodes.borrow()[id]
    }

    fn get_id_by_inode(&self, inode: u64) -> Result<String> {
        self.inodes_to_ids
            .borrow()
            .get(&inode)
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::NoEntry))
            .map(|x| x.to_owned())
    }

    fn get_fuse_stat(&self, dirent: &DirEntity) -> FileAttr {
        let id = &dirent.id;
        let inode = self.get_inode_for_id(id);

        FileAttr {
            ino: inode,
            size: dirent.stat.size,
            blocks: dirent.stat.blocks,
            atime: dirent.stat.atim.into(),
            mtime: dirent.stat.mtim.into(),
            ctime: dirent.stat.ctim.into(),
            crtime: SystemTime::UNIX_EPOCH,
            kind: convert_file_type(dirent.stat.file_type),
            perm: dirent.stat.mode,
            nlink: dirent.stat.nlink as u32,
            uid: dirent.stat.uid,
            gid: dirent.stat.gid,
            rdev: dirent.stat.dev,
            blksize: 0,
            flags: 0,
        }
    }
}

fn convert_file_type(store_file_type: FileType) -> fuser::FileType {
    match store_file_type {
        FileType::NamedPipe => fuser::FileType::NamedPipe,
        FileType::CharDevice => fuser::FileType::CharDevice,
        FileType::BlockDevice => fuser::FileType::BlockDevice,
        FileType::Directory => fuser::FileType::Directory,
        FileType::RegularFile => fuser::FileType::RegularFile,
        FileType::Symlink => fuser::FileType::Symlink,
        FileType::Socket => fuser::FileType::Socket,
    }
}

pub struct FuseOffsFilesystem {
    fs: Arc<RwLock<OffsFilesystem>>,
    rt: Runtime,
    fuse_helper: Arc<Mutex<FuseHelper>>,
}

impl FuseOffsFilesystem {
    pub fn new(fs: Arc<RwLock<OffsFilesystem>>, rt: Runtime) -> Self {
        Self {
            fs,
            rt,
            fuse_helper: Arc::new(Mutex::new(FuseHelper::new())),
        }
    }
}

impl FuseOffsFilesystem {
    fn check_os_str(string: &OsStr) -> Result<&str> {
        string
            .to_str()
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::InvalidValue))
    }

    fn mode_to_file_type(mode: u32) -> FileType {
        if (mode & S_IFIFO) == S_IFIFO {
            FileType::NamedPipe
        } else if (mode & S_IFCHR) == S_IFCHR {
            FileType::CharDevice
        } else if (mode & S_IFBLK) == S_IFBLK {
            FileType::BlockDevice
        } else if (mode & S_IFDIR) == S_IFDIR {
            FileType::Directory
        } else if (mode & S_IFREG) == S_IFREG {
            FileType::RegularFile
        } else if (mode & S_IFLNK) == S_IFLNK {
            FileType::Symlink
        } else if (mode & S_IFSOCK) == S_IFSOCK {
            FileType::Socket
        } else {
            unreachable!()
        }
    }
    fn time_or_now_to_timespec(time_or_now: &TimeOrNow) -> Timespec {
        match time_or_now {
            TimeOrNow::SpecificTime(system_time) => system_time.into(),
            TimeOrNow::Now => SystemTime::now().into(),
        }
    }
}

impl Filesystem for FuseOffsFilesystem {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("Request(lookup): parent={}, name={:?}", parent, name);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();
        let name = name.to_owned();

        self.rt.spawn(async move {
            let parent_id =
                try_fs!(fuse_helper.lock().await.get_id_by_inode(parent), reply).to_owned();
            let mut fs = fs.write().await;

            // Make sure the file entry is up to date
            try_fs!(fs.list_files(&parent_id).await, reply);
            let item = try_fs!(
                fs.query_file_by_name(&parent_id, try_fs!(Self::check_os_str(&name), reply)),
                reply
            );

            let rv = fuse_helper.lock().await.get_fuse_stat(&item);
            debug!("Response: {:?}", rv);
            reply.entry(&TTL, &rv, 1);
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("Request(getattr): ino={}", ino);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();

        self.rt.spawn(async move {
            let id = try_fs!(fuse_helper.lock().await.get_id_by_inode(ino), reply);
            let fs = fs.read().await;

            let item = try_fs!(fs.query_file(&id), reply);

            let rv = fuse_helper.lock().await.get_fuse_stat(&item);
            debug!("Response: {:?}", rv);
            reply.attr(&TTL, &rv);
        });
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!("Request(setattr): ino={}, mode={:?}, uid={:?}, gid={:?}, size={:?}, atime={:?}, mtime={:?}", ino, mode, uid, gid, size, atime, mtime);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();

        self.rt.spawn(async move {
            let id = try_fs!(fuse_helper.lock().await.get_id_by_inode(ino), reply).to_owned();
            let mut fs = fs.write().await;

            let mode = mode.map(|x| x as FileMode);
            let dirent = try_fs!(
                fs.set_attributes(
                    &id,
                    mode,
                    uid,
                    gid,
                    size,
                    atime.map(|x| Self::time_or_now_to_timespec(&x)),
                    mtime.map(|x| Self::time_or_now_to_timespec(&x))
                )
                .await,
                reply
            );

            let rv = fuse_helper.lock().await.get_fuse_stat(&dirent);
            debug!("Response: {:?}", rv);
            reply.attr(&TTL, &rv);
        });
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        debug!("Request(readlink): ino={}", ino);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();

        self.rt.spawn(async move {
            let id = try_fs!(fuse_helper.lock().await.get_id_by_inode(ino), reply).to_owned();
            let mut fs = fs.write().await;

            let dirent = try_fs!(fs.update_dirent(&id, true).await, reply);

            let data = try_fs!(fs.read(&id, 0, dirent.stat.size as u32).await, reply);
            debug!("Response: {:?}", data);
            reply.data(&data);
        });
    }

    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        debug!(
            "Request(mknod): parent={}, name={:?}, mode={}, rdev={}",
            parent, name, mode, rdev
        );

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();
        let name = name.to_owned();

        self.rt.spawn(async move {
            let parent_id =
                try_fs!(fuse_helper.lock().await.get_id_by_inode(parent), reply).clone();
            let mut fs = fs.write().await;

            let dirent = try_fs!(
                fs.create_file(
                    &parent_id,
                    try_fs!(Self::check_os_str(&name), reply),
                    Self::mode_to_file_type(mode),
                    mode as FileMode,
                    rdev,
                )
                .await,
                reply
            );

            let rv = fuse_helper.lock().await.get_fuse_stat(&dirent);
            debug!("Response: {:?}", rv);
            reply.entry(&TTL, &rv, 1);
        });
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        debug!(
            "Request(mkdir): parent={}, name={:?}, mode={}",
            parent, name, mode
        );

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();
        let name = name.to_owned();

        self.rt.spawn(async move {
            let parent_id =
                try_fs!(fuse_helper.lock().await.get_id_by_inode(parent), reply).clone();
            let mut fs = fs.write().await;

            let dirent = try_fs!(
                fs.create_directory(
                    &parent_id,
                    try_fs!(Self::check_os_str(&name), reply),
                    mode as FileMode,
                )
                .await,
                reply
            );

            let rv = fuse_helper.lock().await.get_fuse_stat(&dirent);
            debug!("Response: {:?}", rv);
            reply.entry(&TTL, &rv, 1);
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("Request(unlink): parent={}, name={:?}", parent, name);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();
        let name = name.to_owned();

        self.rt.spawn(async move {
            let parent_id = try_fs!(fuse_helper.lock().await.get_id_by_inode(parent), reply);
            let mut fs = fs.write().await;

            let item = try_fs!(
                fs.query_file_by_name(&parent_id, try_fs!(Self::check_os_str(&name), reply)),
                reply
            );

            try_fs!(fs.remove_file(&item.id).await, reply);

            debug!("Response: ok");
            reply.ok();
        });
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("Request(rmdir): parent={}, name={:?}", parent, name);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();
        let name = name.to_owned();

        self.rt.spawn(async move {
            let parent_id = try_fs!(fuse_helper.lock().await.get_id_by_inode(parent), reply);
            let mut fs = fs.write().await;

            let item = try_fs!(
                fs.query_file_by_name(&parent_id, try_fs!(Self::check_os_str(&name), reply)),
                reply
            );

            try_fs!(fs.remove_directory(&item.id).await, reply);

            debug!("Response: ok");
            reply.ok();
        });
    }

    fn symlink(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        debug!(
            "Request(symlink): parent={}, name={:?}, link={:?}",
            parent, name, link
        );

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();
        let name = name.to_owned();
        let link = link.to_owned();

        self.rt.spawn(async move {
            let parent_id =
                try_fs!(fuse_helper.lock().await.get_id_by_inode(parent), reply).clone();
            let mut fs = fs.write().await;

            let dirent = try_fs!(
                fs.create_symlink(
                    &parent_id,
                    try_fs!(Self::check_os_str(&name), reply),
                    try_fs!(Self::check_os_str(link.as_os_str()), reply),
                )
                .await,
                reply
            );

            let rv = fuse_helper.lock().await.get_fuse_stat(&dirent);
            debug!("Response: {:?}", rv);
            reply.entry(&TTL, &rv, 1);
        });
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        debug!(
            "Request(rename): parent={}, name={:?}, newparent={}, newname={:?}",
            parent, name, newparent, newname
        );

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();
        let name = name.to_owned();
        let newname = newname.to_owned();

        self.rt.spawn(async move {
            let old_parent_id = try_fs!(fuse_helper.lock().await.get_id_by_inode(parent), reply);
            let new_parent_id =
                try_fs!(fuse_helper.lock().await.get_id_by_inode(newparent), reply).clone();
            let mut fs = fs.write().await;

            let item = try_fs!(
                fs.query_file_by_name(&old_parent_id, try_fs!(Self::check_os_str(&name), reply)),
                reply
            );

            try_fs!(
                fs.rename_file(
                    &item.id,
                    &new_parent_id,
                    try_fs!(Self::check_os_str(&newname), reply),
                )
                .await,
                reply
            );

            debug!("Response: ok");
            reply.ok();
        });
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        debug!("Request(open): ino={}", ino);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let id = try_fs!(fuse_helper.lock().await.get_id_by_inode(ino), reply).clone();

            try_fs!(fs.update_dirent(&id, true).await, reply);
            try_fs!(fs.update_chunks(&id).await, reply);

            let fh = fs.open_file_handler.open_file(id);
            let flags: u32 = 0;
            debug!("Response: fh={}, flags={}", fh, flags);
            reply.opened(fh, flags);
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        debug!(
            "Request(read): ino={}, offset={}, size={}",
            ino, offset, size
        );

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            try_fs!(fs.flush_write_buffer(fh).await, reply);
            let id = fuse_helper
                .lock()
                .await
                .get_id_by_inode(ino)
                .unwrap()
                .clone();

            let data = try_fs!(fs.read(&id, offset, size).await, reply);
            debug!("Response: {:?}", data);
            reply.data(&data);
        });
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        debug!(
            "Request(read): ino={}, offset={}, data={:?}",
            ino, offset, data
        );

        let fs = self.fs.clone();
        let data = data.to_vec();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let rv = data.len() as u32;
            try_fs!(fs.write(fh, offset, data).await, reply);
            debug!("Response: {:?}", rv);
            reply.written(rv);
        });
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("Request(release): ino={}", ino);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();

        self.rt.spawn(async move {
            let id = try_fs!(fuse_helper.lock().await.get_id_by_inode(ino), reply).clone();
            let mut fs = fs.write().await;

            try_fs!(fs.flush_write_buffer(fh).await, reply);
            try_fs!(fs.update_dirent(&id, true).await, reply);
            fs.open_file_handler.close_file(fh);

            debug!("Response: ok");
            reply.ok();
        });
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        debug!("Request(fsync)");

        let fs = self.fs.clone();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            try_fs!(fs.flush_write_buffer(fh).await, reply);

            debug!("Response: ok");
            reply.ok();
        });
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("Request(readdir): ino={}, offset={}", ino, offset);

        let fs = self.fs.clone();
        let fuse_helper = self.fuse_helper.clone();

        self.rt.spawn(async move {
            let dir_id = try_fs!(fuse_helper.lock().await.get_id_by_inode(ino), reply).clone();
            let mut fs = fs.write().await;

            try_fs!(fs.update_dirent(&dir_id, true).await, reply);
            let items = try_fs!(fs.list_files(&dir_id).await, reply);

            let mut entries = vec![
                (1, fuser::FileType::Directory, ".".to_owned()),
                (1, fuser::FileType::Directory, "..".to_owned()),
            ];
            {
                let fuse_helper_locked = fuse_helper.lock().await;
                for dirent in items {
                    entries.push((
                        fuse_helper_locked.get_inode_for_id(&dirent.id),
                        convert_file_type(dirent.stat.file_type),
                        dirent.name,
                    ));
                }
            }

            let to_skip = if offset == 0 { offset } else { offset + 1 } as usize;
            for (i, entry) in entries.into_iter().enumerate().skip(to_skip) {
                reply.add(entry.0, i as i64, entry.1, &entry.2);
            }
            debug!("Response: ok");
            reply.ok();
        });
    }
}

impl Drop for FuseOffsFilesystem {
    fn drop(&mut self) {
        let fs = self.fs.clone();
        self.rt.block_on(async move {
            fs.write().await.close_all_files().await.unwrap();
        });
    }
}
