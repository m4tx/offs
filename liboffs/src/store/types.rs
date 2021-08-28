use crate::timespec::Timespec;
use num_derive::{FromPrimitive, ToPrimitive};

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

#[derive(Clone, Copy, Debug)]
pub struct FileStat {
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
