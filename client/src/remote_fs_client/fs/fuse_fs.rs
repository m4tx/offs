use std::ffi::OsStr;
use std::path::Path;

use fuse::{
    FileAttr, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite, Request,
};
use libc::{S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFREG, S_IFSOCK};
use time::Timespec;

use offs::store::{DirEntity, FileMode, FileType};

use super::error::{RemoteFsError, RemoteFsErrorKind};
use super::OffsFilesystem;
use super::Result;

const TTL: time::Timespec = Timespec { sec: 1, nsec: 0 };

macro_rules! try_fs {
    ($e:expr, $reply:ident) => {
        match $e {
            Ok(val) => val,
            Err(e) => {
                $reply.error(e.to_os_error());
                return;
            }
        }
    };
}

impl OffsFilesystem {
    fn check_os_str(string: &OsStr) -> Result<&str> {
        string
            .to_str()
            .ok_or(RemoteFsError::new(RemoteFsErrorKind::InvalidValue))
    }

    fn get_fuse_stat(&mut self, dirent: &DirEntity) -> FileAttr {
        let id = &dirent.id;
        let inode = self.get_inode_for_id(id);

        FileAttr {
            ino: inode,
            size: dirent.stat.size,
            blocks: dirent.stat.blocks,
            atime: dirent.stat.atim,
            mtime: dirent.stat.mtim,
            ctime: dirent.stat.ctim,
            crtime: Timespec { sec: 0, nsec: 0 },
            kind: OffsFilesystem::convert_file_type(dirent.stat.file_type),
            perm: dirent.stat.mode,
            nlink: dirent.stat.nlink as u32,
            uid: dirent.stat.uid,
            gid: dirent.stat.gid,
            rdev: dirent.stat.dev,
            flags: 0,
        }
    }

    fn convert_file_type(store_file_type: FileType) -> fuse::FileType {
        match store_file_type {
            FileType::NamedPipe => fuse::FileType::NamedPipe,
            FileType::CharDevice => fuse::FileType::CharDevice,
            FileType::BlockDevice => fuse::FileType::BlockDevice,
            FileType::Directory => fuse::FileType::Directory,
            FileType::RegularFile => fuse::FileType::RegularFile,
            FileType::Symlink => fuse::FileType::Symlink,
            FileType::Socket => fuse::FileType::Socket,
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
}

impl Filesystem for OffsFilesystem {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_id = try_fs!(self.get_id_by_inode(parent), reply);
        let item = try_fs!(
            self.query_file_by_name(&parent_id, try_fs!(Self::check_os_str(name), reply)),
            reply
        );

        reply.entry(&TTL, &self.get_fuse_stat(&item), 1);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let id = try_fs!(self.get_id_by_inode(ino), reply);
        let item = try_fs!(self.query_file(&id), reply);

        reply.attr(&TTL, &self.get_fuse_stat(&item));
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let id = try_fs!(self.get_id_by_inode(ino), reply).to_owned();

        let mode = mode.map(|x| x as FileMode);
        let dirent = try_fs!(
            self.set_attributes(&id, mode, uid, gid, size, atime, mtime),
            reply
        );

        reply.attr(&TTL, &self.get_fuse_stat(&dirent));
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        let id = try_fs!(self.get_id_by_inode(ino), reply).to_owned();
        let dirent = try_fs!(self.update_dirent(&id, true), reply);

        let data = try_fs!(self.read(&id, 0, dirent.stat.size as u32), reply);

        reply.data(&data);
    }

    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let parent_id = try_fs!(self.get_id_by_inode(parent), reply).clone();

        let dirent = try_fs!(
            self.create_file(
                &parent_id,
                try_fs!(Self::check_os_str(name), reply),
                Self::mode_to_file_type(mode),
                mode as FileMode,
                rdev,
            ),
            reply
        );

        reply.entry(&TTL, &self.get_fuse_stat(&dirent), 1);
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        let parent_id = try_fs!(self.get_id_by_inode(parent), reply).clone();

        let dirent = try_fs!(
            self.create_directory(
                &parent_id,
                try_fs!(Self::check_os_str(name), reply),
                mode as FileMode,
            ),
            reply
        );

        reply.entry(&TTL, &self.get_fuse_stat(&dirent), 1);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_id = try_fs!(self.get_id_by_inode(parent), reply);
        let item = try_fs!(
            self.query_file_by_name(&parent_id, try_fs!(Self::check_os_str(name), reply)),
            reply
        );

        try_fs!(self.remove_file(&item.id), reply);

        reply.ok();
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_id = try_fs!(self.get_id_by_inode(parent), reply);
        let item = try_fs!(
            self.query_file_by_name(&parent_id, try_fs!(Self::check_os_str(name), reply)),
            reply
        );

        try_fs!(self.remove_directory(&item.id), reply);

        reply.ok();
    }

    fn symlink(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        let parent_id = try_fs!(self.get_id_by_inode(parent), reply).clone();

        let dirent = try_fs!(
            self.create_symlink(
                &parent_id,
                try_fs!(Self::check_os_str(name), reply),
                try_fs!(Self::check_os_str(link.as_os_str()), reply),
            ),
            reply
        );

        reply.entry(&TTL, &self.get_fuse_stat(&dirent), 1);
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        let old_parent_id = try_fs!(self.get_id_by_inode(parent), reply);
        let new_parent_id = try_fs!(self.get_id_by_inode(newparent), reply).clone();
        let item = try_fs!(
            self.query_file_by_name(&old_parent_id, try_fs!(Self::check_os_str(name), reply)),
            reply
        );

        try_fs!(
            self.rename_file(
                &item.id,
                &new_parent_id,
                try_fs!(Self::check_os_str(newname), reply),
            ),
            reply
        );

        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        try_fs!(self.flush_write_buffer(), reply);
        let id = try_fs!(self.get_id_by_inode(ino), reply).clone();

        try_fs!(self.update_dirent(&id, true), reply);
        try_fs!(self.update_chunks(&id), reply);

        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        try_fs!(self.flush_write_buffer(), reply);
        let id = self.inodes_to_ids[&ino].clone();

        let data = try_fs!(self.read(&id, offset, size), reply);

        reply.data(&data);
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        let id = try_fs!(self.get_id_by_inode(ino), reply).clone();

        try_fs!(self.write(&id, offset, data), reply);

        reply.written(data.len() as u32);
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let id = try_fs!(self.get_id_by_inode(ino), reply).clone();

        try_fs!(self.update_dirent(&id, true), reply);
        try_fs!(self.flush_write_buffer(), reply);

        reply.ok();
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        try_fs!(self.flush_write_buffer(), reply);

        reply.ok();
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let dir_id = try_fs!(self.get_id_by_inode(ino), reply).clone();

        try_fs!(self.update_dirent(&dir_id, true), reply);
        let items = try_fs!(self.list_files(&dir_id), reply);

        let mut entries = vec![
            (1, fuse::FileType::Directory, ".".to_owned()),
            (1, fuse::FileType::Directory, "..".to_owned()),
        ];
        for dirent in items {
            entries.push((
                self.get_inode_for_id(&dirent.id),
                OffsFilesystem::convert_file_type(dirent.stat.file_type),
                dirent.name,
            ));
        }

        let to_skip = if offset == 0 { offset } else { offset + 1 } as usize;
        for (i, entry) in entries.into_iter().enumerate().skip(to_skip) {
            reply.add(entry.0, i as i64, entry.1, &entry.2);
        }
        reply.ok();
    }
}
