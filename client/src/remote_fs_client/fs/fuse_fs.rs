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
use tokio::sync::RwLock;

use offs::store::{DirEntity, FileMode, FileType};
use offs::timespec::Timespec;

use super::error::{RemoteFsError, RemoteFsErrorKind};
use super::OffsFilesystem;
use super::Result;

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

pub struct FuseOffsFilesystem {
    fs: Arc<RwLock<OffsFilesystem>>,
    rt: Runtime,
}

impl FuseOffsFilesystem {
    pub fn new(fs: Arc<RwLock<OffsFilesystem>>, rt: Runtime) -> Self {
        Self { fs, rt }
    }
}

impl FuseOffsFilesystem {
    fn check_os_str(string: &OsStr) -> Result<&str> {
        string
            .to_str()
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::InvalidValue))
    }

    fn get_fuse_stat(fs: &mut OffsFilesystem, dirent: &DirEntity) -> FileAttr {
        let id = &dirent.id;
        let inode = fs.get_inode_for_id(id);

        FileAttr {
            ino: inode,
            size: dirent.stat.size,
            blocks: dirent.stat.blocks,
            atime: Self::timespec_to_system_time(&dirent.stat.atim),
            mtime: Self::timespec_to_system_time(&dirent.stat.mtim),
            ctime: Self::timespec_to_system_time(&dirent.stat.ctim),
            crtime: SystemTime::UNIX_EPOCH,
            kind: Self::convert_file_type(dirent.stat.file_type),
            perm: dirent.stat.mode,
            nlink: dirent.stat.nlink as u32,
            uid: dirent.stat.uid,
            gid: dirent.stat.gid,
            rdev: dirent.stat.dev,
            blksize: 0,
            flags: 0,
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

    fn timespec_to_system_time(timespec: &Timespec) -> SystemTime {
        SystemTime::UNIX_EPOCH
            + Duration::from_secs(timespec.sec as u64)
            + Duration::from_nanos(timespec.nsec as u64)
    }

    fn system_time_to_timespec(system_time: &SystemTime) -> Timespec {
        let duration = system_time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        Timespec::new(duration.as_secs() as i64, duration.subsec_nanos())
    }

    fn time_or_now_to_timespec(time_or_now: &TimeOrNow) -> Timespec {
        match time_or_now {
            TimeOrNow::SpecificTime(system_time) => Self::system_time_to_timespec(system_time),
            TimeOrNow::Now => Self::system_time_to_timespec(&SystemTime::now()),
        }
    }
}

impl Filesystem for FuseOffsFilesystem {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("Request(lookup): parent={}, name={:?}", parent, name);

        let fs = self.fs.clone();
        let name = name.to_owned();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let parent_id = try_fs!(fs.get_id_by_inode(parent), reply);
            let item = try_fs!(
                fs.query_file_by_name(&parent_id, try_fs!(Self::check_os_str(&name), reply)),
                reply
            );

            let rv = Self::get_fuse_stat(&mut fs, &item);
            debug!("Response: {:?}", rv);
            reply.entry(&TTL, &rv, 1);
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("Request(getattr): ino={}", ino);

        let fs = self.fs.clone();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let id = try_fs!(fs.get_id_by_inode(ino), reply);
            let item = try_fs!(fs.query_file(&id), reply);

            let rv = Self::get_fuse_stat(&mut fs, &item);
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

        self.rt.spawn(async move {
            let mut fs = fs.write().await;
            let id = try_fs!(fs.get_id_by_inode(ino), reply).to_owned();

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

            let rv = Self::get_fuse_stat(&mut fs, &dirent);
            debug!("Response: {:?}", rv);
            reply.attr(&TTL, &rv);
        });
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        debug!("Request(readlink): ino={}", ino);

        let fs = self.fs.clone();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let id = try_fs!(fs.get_id_by_inode(ino), reply).to_owned();
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
        let name = name.to_owned();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let parent_id = try_fs!(fs.get_id_by_inode(parent), reply).clone();

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

            let rv = Self::get_fuse_stat(&mut fs, &dirent);
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
        let name = name.to_owned();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let parent_id = try_fs!(fs.get_id_by_inode(parent), reply).clone();

            let dirent = try_fs!(
                fs.create_directory(
                    &parent_id,
                    try_fs!(Self::check_os_str(&name), reply),
                    mode as FileMode,
                )
                .await,
                reply
            );

            let rv = Self::get_fuse_stat(&mut fs, &dirent);
            debug!("Response: {:?}", rv);
            reply.entry(&TTL, &rv, 1);
        });
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("Request(unlink): parent={}, name={:?}", parent, name);

        let fs = self.fs.clone();
        let name = name.to_owned();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let parent_id = try_fs!(fs.get_id_by_inode(parent), reply);
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
        let name = name.to_owned();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let parent_id = try_fs!(fs.get_id_by_inode(parent), reply);
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
        let name = name.to_owned();
        let link = link.to_owned();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let parent_id = try_fs!(fs.get_id_by_inode(parent), reply).clone();

            let dirent = try_fs!(
                fs.create_symlink(
                    &parent_id,
                    try_fs!(Self::check_os_str(&name), reply),
                    try_fs!(Self::check_os_str(link.as_os_str()), reply),
                )
                .await,
                reply
            );

            let rv = Self::get_fuse_stat(&mut fs, &dirent);
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
        let name = name.to_owned();
        let newname = newname.to_owned();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let old_parent_id = try_fs!(fs.get_id_by_inode(parent), reply);
            let new_parent_id = try_fs!(fs.get_id_by_inode(newparent), reply).clone();
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

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            try_fs!(fs.flush_write_buffer().await, reply);
            let id = try_fs!(fs.get_id_by_inode(ino), reply).clone();

            try_fs!(fs.update_dirent(&id, true).await, reply);
            try_fs!(fs.update_chunks(&id).await, reply);

            let fh: u64 = 0;
            let flags: u32 = 0;
            debug!("Response: fh={}, flags={}", fh, flags);
            reply.opened(fh, flags);
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
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

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            try_fs!(fs.flush_write_buffer().await, reply);
            let id = fs.inodes_to_ids[&ino].clone();

            let data = try_fs!(fs.read(&id, offset, size).await, reply);
            debug!("Response: {:?}", data);
            reply.data(&data);
        });
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
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

            let id = try_fs!(fs.get_id_by_inode(ino), reply).clone();

            try_fs!(fs.write(&id, offset, &data).await, reply);
            let rv = data.len() as u32;
            debug!("Response: {:?}", rv);
            reply.written(rv);
        });
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!("Request(release): ino={}", ino);

        let fs = self.fs.clone();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let id = try_fs!(fs.get_id_by_inode(ino), reply).clone();

            try_fs!(fs.update_dirent(&id, true).await, reply);
            try_fs!(fs.flush_write_buffer().await, reply);

            debug!("Response: ok");
            reply.ok();
        });
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        debug!("Request(fsync)");

        let fs = self.fs.clone();

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            try_fs!(fs.flush_write_buffer().await, reply);

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

        self.rt.spawn(async move {
            let mut fs = fs.write().await;

            let dir_id = try_fs!(fs.get_id_by_inode(ino), reply).clone();

            try_fs!(fs.update_dirent(&dir_id, true).await, reply);
            let items = try_fs!(fs.list_files(&dir_id).await, reply);

            let mut entries = vec![
                (1, fuser::FileType::Directory, ".".to_owned()),
                (1, fuser::FileType::Directory, "..".to_owned()),
            ];
            for dirent in items {
                entries.push((
                    fs.get_inode_for_id(&dirent.id),
                    Self::convert_file_type(dirent.stat.file_type),
                    dirent.name,
                ));
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
            fs.write().await.flush_write_buffer().await.unwrap();
        });
    }
}
