use crate::store::{FileDev, FileMode, FileType};
use time::Timespec;

#[derive(Clone)]
pub struct CreateFileOperation {
    pub name: String,
    pub file_type: FileType,
    pub perm: FileMode,
    pub dev: FileDev,
}

#[derive(Clone)]
pub struct CreateSymlinkOperation {
    pub name: String,
    pub link: String,
}

#[derive(Clone)]
pub struct CreateDirectoryOperation {
    pub name: String,
    pub perm: FileMode,
}

#[derive(Clone)]
pub struct RemoveFileOperation {}

#[derive(Clone)]
pub struct RemoveDirectoryOperation {}

#[derive(Clone)]
pub struct RenameOperation {
    pub new_parent: String,
    pub new_name: String,
}

#[derive(Clone)]
pub struct SetAttributesOperation {
    pub perm: Option<FileMode>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atim: Option<Timespec>,
    pub mtim: Option<Timespec>,
}

#[derive(Clone)]
pub struct WriteOperation {
    pub offset: i64,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub enum ModifyOperationContent {
    CreateFileOperation(CreateFileOperation),
    CreateSymlinkOperation(CreateSymlinkOperation),
    CreateDirectoryOperation(CreateDirectoryOperation),

    RemoveFileOperation(RemoveFileOperation),
    RemoveDirectoryOperation(RemoveDirectoryOperation),

    RenameOperation(RenameOperation),
    SetAttributesOperation(SetAttributesOperation),
    WriteOperation(WriteOperation),
}

#[derive(Clone)]
pub struct ModifyOperation {
    pub id: String,
    pub timestamp: Timespec,

    pub dirent_version: i64,
    pub content_version: i64,

    pub operation: ModifyOperationContent,
}
