use num_derive::{FromPrimitive, ToPrimitive};
use time::Timespec;

pub type FileMode = u16;
pub type FileDev = u32;

#[derive(Clone, Copy, Debug, Hash, PartialEq, FromPrimitive, ToPrimitive)]
pub enum FileType {
    NamedPipe = 0,
    CharDevice,
    BlockDevice,
    Directory,
    RegularFile,
    Symlink,
    Socket,
}

impl Into<crate::filesystem_capnp::FileType> for FileType {
    fn into(self) -> crate::filesystem_capnp::FileType {
        match self {
            FileType::NamedPipe => crate::filesystem_capnp::FileType::NamedPipe,
            FileType::CharDevice => crate::filesystem_capnp::FileType::CharDevice,
            FileType::BlockDevice => crate::filesystem_capnp::FileType::BlockDevice,
            FileType::Directory => crate::filesystem_capnp::FileType::Directory,
            FileType::RegularFile => crate::filesystem_capnp::FileType::RegularFile,
            FileType::Symlink => crate::filesystem_capnp::FileType::Symlink,
            FileType::Socket => crate::filesystem_capnp::FileType::Socket,
        }
    }
}

impl From<crate::filesystem_capnp::FileType> for FileType {
    fn from(proto_file_type: crate::filesystem_capnp::FileType) -> Self {
        match proto_file_type {
            crate::filesystem_capnp::FileType::NamedPipe => FileType::NamedPipe,
            crate::filesystem_capnp::FileType::CharDevice => FileType::CharDevice,
            crate::filesystem_capnp::FileType::BlockDevice => FileType::BlockDevice,
            crate::filesystem_capnp::FileType::Directory => FileType::Directory,
            crate::filesystem_capnp::FileType::RegularFile => FileType::RegularFile,
            crate::filesystem_capnp::FileType::Symlink => FileType::Symlink,
            crate::filesystem_capnp::FileType::Socket => FileType::Socket,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FileStat {
    pub ino: u64,
    pub file_type: FileType,
    pub mode: FileMode,
    pub dev: FileDev,
    pub nlink: u64,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub blocks: u64,
    pub atim: Timespec,
    pub mtim: Timespec,
    pub ctim: Timespec,
}

impl FileStat {
    pub fn has_size(&self) -> bool {
        self.file_type == FileType::RegularFile
    }
}

impl<'a> From<crate::filesystem_capnp::stat::Reader<'a>> for FileStat {
    fn from(proto_stat: crate::filesystem_capnp::stat::Reader<'a>) -> Self {
        FileStat {
            ino: proto_stat.get_ino(),
            file_type: proto_stat.get_file_type().unwrap().into(),
            mode: proto_stat.get_mode(),
            dev: proto_stat.get_dev(),
            nlink: proto_stat.get_nlink(),
            uid: proto_stat.get_uid(),
            gid: proto_stat.get_gid(),
            size: proto_stat.get_size(),
            blocks: proto_stat.get_blocks(),
            atim: proto_stat.get_atim().unwrap().into(),
            mtim: proto_stat.get_mtim().unwrap().into(),
            ctim: proto_stat.get_ctim().unwrap().into(),
        }
    }
}

pub trait ProtoFill<T> {
    fn fill_proto(&self, proto: T);
}

impl<'a> ProtoFill<crate::filesystem_capnp::timespec::Builder<'a>> for Timespec {
    fn fill_proto(&self, mut timespec: crate::filesystem_capnp::timespec::Builder<'a>) {
        timespec.set_sec(self.sec);
        timespec.set_nsec(self.nsec);
    }
}

impl<'a> ProtoFill<crate::filesystem_capnp::stat::Builder<'a>> for FileStat {
    fn fill_proto(&self, mut stat: crate::filesystem_capnp::stat::Builder<'a>) {
        stat.set_ino(self.ino);
        stat.set_file_type(self.file_type.into());
        stat.set_mode(self.mode);
        stat.set_nlink(self.nlink);
        stat.set_uid(self.uid);
        stat.set_gid(self.gid);
        stat.set_size(self.size);
        stat.set_blocks(self.blocks);

        self.atim.fill_proto(stat.reborrow().init_atim());
        self.mtim.fill_proto(stat.reborrow().init_mtim());
        self.ctim.fill_proto(stat.reborrow().init_ctim());
    }
}

#[derive(Clone, Debug)]
pub struct DirEntity {
    pub id: String,
    pub parent: String,
    pub name: String,

    pub dirent_version: i64,
    pub content_version: i64,
    pub retrieved_version: i64,

    pub stat: FileStat,
}

impl DirEntity {
    pub fn is_retrieved(&self) -> bool {
        self.retrieved_version != 0
    }

    pub fn is_up_to_date(&self) -> bool {
        self.retrieved_version == self.content_version
    }
}

impl<'a> From<crate::filesystem_capnp::dir_entity::Reader<'a>> for DirEntity {
    fn from(proto_dirent: crate::filesystem_capnp::dir_entity::Reader<'a>) -> Self {
        DirEntity {
            id: proto_dirent.get_id().unwrap().to_owned(),
            parent: proto_dirent.get_parent().unwrap().to_owned(),
            name: proto_dirent.get_name().unwrap().to_owned(),

            dirent_version: proto_dirent.get_dirent_version(),
            content_version: proto_dirent.get_content_version(),
            retrieved_version: 0,

            stat: proto_dirent.get_stat().unwrap().into(),
        }
    }
}

impl<'a> ProtoFill<crate::filesystem_capnp::dir_entity::Builder<'a>> for DirEntity {
    fn fill_proto(&self, mut dirent: crate::filesystem_capnp::dir_entity::Builder<'a>) {
        dirent.set_id(&self.id);
        dirent.set_parent(&self.parent);
        dirent.set_name(&self.name);

        dirent.set_dirent_version(self.dirent_version);
        dirent.set_content_version(self.content_version);

        dirent.reborrow().init_stat();
        let stat = dirent.get_stat().unwrap();

        self.stat.fill_proto(stat);
    }
}
