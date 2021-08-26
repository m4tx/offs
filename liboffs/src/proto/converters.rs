use itertools::Itertools;
use num_traits::cast::FromPrimitive;

use crate::errors::{JournalApplyData, JournalApplyResult, OperationApplyError};
use crate::modify_op;
use crate::modify_op::ModifyOperationContent;
use crate::proto::filesystem::apply_journal_response::Error;
use crate::proto::filesystem::modify_operation::Operation;
use crate::proto::filesystem::FileChunks;
use crate::store as crate_types;
use crate::store::{FileMode, FileType};

use super::filesystem as proto_types;

// Timespec
impl Into<proto_types::Timespec> for crate::timespec::Timespec {
    fn into(self) -> proto_types::Timespec {
        proto_types::Timespec {
            sec: self.sec,
            nsec: self.nsec as i32,
        }
    }
}

impl Into<crate::timespec::Timespec> for proto_types::Timespec {
    fn into(self) -> crate::timespec::Timespec {
        crate::timespec::Timespec::new(self.sec, self.nsec as u32)
    }
}

// UInt32Value
impl From<u32> for proto_types::UInt32Value {
    fn from(value: u32) -> Self {
        proto_types::UInt32Value { value }
    }
}

impl Into<u32> for proto_types::UInt32Value {
    fn into(self) -> u32 {
        self.value
    }
}

// UInt64Value
impl From<u64> for proto_types::UInt64Value {
    fn from(value: u64) -> Self {
        proto_types::UInt64Value { value }
    }
}

impl Into<u64> for proto_types::UInt64Value {
    fn into(self) -> u64 {
        self.value
    }
}

// FileType
impl From<crate_types::FileType> for proto_types::FileType {
    fn from(value: crate_types::FileType) -> Self {
        match value {
            crate_types::FileType::NamedPipe => proto_types::FileType::NamedPipe,
            crate_types::FileType::CharDevice => proto_types::FileType::CharDevice,
            crate_types::FileType::BlockDevice => proto_types::FileType::BlockDevice,
            crate_types::FileType::Directory => proto_types::FileType::Directory,
            crate_types::FileType::RegularFile => proto_types::FileType::RegularFile,
            crate_types::FileType::Symlink => proto_types::FileType::Symlink,
            crate_types::FileType::Socket => proto_types::FileType::Socket,
        }
    }
}

impl From<proto_types::FileType> for crate_types::FileType {
    fn from(value: proto_types::FileType) -> Self {
        match value {
            proto_types::FileType::NamedPipe => crate_types::FileType::NamedPipe,
            proto_types::FileType::CharDevice => crate_types::FileType::CharDevice,
            proto_types::FileType::BlockDevice => crate_types::FileType::BlockDevice,
            proto_types::FileType::Directory => crate_types::FileType::Directory,
            proto_types::FileType::RegularFile => crate_types::FileType::RegularFile,
            proto_types::FileType::Symlink => crate_types::FileType::Symlink,
            proto_types::FileType::Socket => crate_types::FileType::Socket,
        }
    }
}

// Stat
impl From<crate_types::FileStat> for proto_types::Stat {
    fn from(value: crate_types::FileStat) -> Self {
        proto_types::Stat {
            ino: value.ino,
            file_type: value.file_type as i32,
            perm: value.mode as u32,
            nlink: value.nlink,
            uid: value.uid,
            gid: value.gid,
            size: value.size,
            blocks: value.blocks,
            atim: Some(value.atim.into()),
            mtim: Some(value.mtim.into()),
            ctim: Some(value.ctim.into()),
        }
    }
}

impl From<proto_types::Stat> for crate_types::FileStat {
    fn from(value: proto_types::Stat) -> Self {
        crate_types::FileStat {
            ino: value.ino,
            file_type: FileType::from_i32(value.file_type).unwrap(),
            mode: value.perm as u16,
            dev: 0,
            nlink: value.nlink,
            uid: value.uid,
            gid: value.gid,
            size: value.size,
            blocks: value.blocks,
            atim: value.atim.unwrap().into(),
            mtim: value.mtim.unwrap().into(),
            ctim: value.ctim.unwrap().into(),
        }
    }
}

// DirEntity
impl From<crate_types::DirEntity> for proto_types::DirEntity {
    fn from(value: crate_types::DirEntity) -> Self {
        proto_types::DirEntity {
            id: value.id,
            parent: value.parent,
            name: value.name,
            dirent_version: value.dirent_version,
            content_version: value.content_version,
            stat: Some(value.stat.into()),
        }
    }
}

impl From<proto_types::DirEntity> for crate_types::DirEntity {
    fn from(value: proto_types::DirEntity) -> Self {
        crate_types::DirEntity {
            id: value.id,
            parent: value.parent,
            name: value.name,

            dirent_version: value.dirent_version,
            content_version: value.content_version,
            retrieved_version: 0,

            stat: value.stat.unwrap().into(),
        }
    }
}

// CreateFileOperation
impl From<modify_op::CreateFileOperation> for proto_types::CreateFileOperation {
    fn from(value: modify_op::CreateFileOperation) -> Self {
        proto_types::CreateFileOperation {
            name: value.name,
            file_type: value.file_type as i32,
            perm: value.perm as u32,
            dev: value.dev,
        }
    }
}

impl From<proto_types::CreateFileOperation> for modify_op::CreateFileOperation {
    fn from(value: proto_types::CreateFileOperation) -> Self {
        modify_op::CreateFileOperation {
            name: value.name,
            file_type: FileType::from_i32(value.file_type).unwrap(),
            perm: value.perm as u16,
            dev: value.dev,
        }
    }
}

// CreateSymlinkOperation
impl From<modify_op::CreateSymlinkOperation> for proto_types::CreateSymlinkOperation {
    fn from(value: modify_op::CreateSymlinkOperation) -> Self {
        proto_types::CreateSymlinkOperation {
            name: value.name,
            link: value.link,
        }
    }
}

impl From<proto_types::CreateSymlinkOperation> for modify_op::CreateSymlinkOperation {
    fn from(value: proto_types::CreateSymlinkOperation) -> Self {
        modify_op::CreateSymlinkOperation {
            name: value.name,
            link: value.link,
        }
    }
}

// CreateDirectoryOperation
impl From<modify_op::CreateDirectoryOperation> for proto_types::CreateDirectoryOperation {
    fn from(value: modify_op::CreateDirectoryOperation) -> Self {
        proto_types::CreateDirectoryOperation {
            name: value.name,
            perm: value.perm as u32,
        }
    }
}

impl From<proto_types::CreateDirectoryOperation> for modify_op::CreateDirectoryOperation {
    fn from(value: proto_types::CreateDirectoryOperation) -> Self {
        modify_op::CreateDirectoryOperation {
            name: value.name,
            perm: value.perm as u16,
        }
    }
}

// RemoveFileOperation
impl From<modify_op::RemoveFileOperation> for proto_types::RemoveFileOperation {
    fn from(_value: modify_op::RemoveFileOperation) -> Self {
        proto_types::RemoveFileOperation {}
    }
}

impl From<proto_types::RemoveFileOperation> for modify_op::RemoveFileOperation {
    fn from(_value: proto_types::RemoveFileOperation) -> Self {
        modify_op::RemoveFileOperation {}
    }
}

// RemoveDirectoryOperation
impl From<modify_op::RemoveDirectoryOperation> for proto_types::RemoveDirectoryOperation {
    fn from(_value: modify_op::RemoveDirectoryOperation) -> Self {
        proto_types::RemoveDirectoryOperation {}
    }
}

impl From<proto_types::RemoveDirectoryOperation> for modify_op::RemoveDirectoryOperation {
    fn from(_value: proto_types::RemoveDirectoryOperation) -> Self {
        modify_op::RemoveDirectoryOperation {}
    }
}

// RenameOperation
impl From<modify_op::RenameOperation> for proto_types::RenameOperation {
    fn from(value: modify_op::RenameOperation) -> Self {
        proto_types::RenameOperation {
            new_parent: value.new_parent,
            new_name: value.new_name,
        }
    }
}

impl From<proto_types::RenameOperation> for modify_op::RenameOperation {
    fn from(value: proto_types::RenameOperation) -> Self {
        modify_op::RenameOperation {
            new_parent: value.new_parent,
            new_name: value.new_name,
        }
    }
}

// SetAttributesOperation
impl From<modify_op::SetAttributesOperation> for proto_types::SetAttributesOperation {
    fn from(value: modify_op::SetAttributesOperation) -> Self {
        proto_types::SetAttributesOperation {
            perm: value.perm.map(|x| (x as u32).into()),
            uid: value.uid.map(|x| x.into()),
            gid: value.gid.map(|x| x.into()),
            size: value.size.map(|x| x.into()),
            atim: value.atim.map(|x| x.into()),
            mtim: value.mtim.map(|x| x.into()),
        }
    }
}

impl From<proto_types::SetAttributesOperation> for modify_op::SetAttributesOperation {
    fn from(value: proto_types::SetAttributesOperation) -> Self {
        Self {
            perm: value.perm.map(|x| Into::<u32>::into(x) as FileMode),
            uid: value.uid.map(|x| x.into()),
            gid: value.gid.map(|x| x.into()),
            size: value.size.map(|x| x.into()),
            atim: value.atim.map(|x| x.into()),
            mtim: value.mtim.map(|x| x.into()),
        }
    }
}

// CreateFileOperation
impl From<modify_op::WriteOperation> for proto_types::WriteOperation {
    fn from(value: modify_op::WriteOperation) -> Self {
        proto_types::WriteOperation {
            offset: value.offset,
            data: value.data,
        }
    }
}

impl From<proto_types::WriteOperation> for modify_op::WriteOperation {
    fn from(value: proto_types::WriteOperation) -> Self {
        modify_op::WriteOperation {
            offset: value.offset,
            data: value.data,
        }
    }
}

// ModifyOperation
impl From<modify_op::ModifyOperation> for proto_types::ModifyOperation {
    fn from(value: modify_op::ModifyOperation) -> Self {
        proto_types::ModifyOperation {
            id: value.id,
            timestamp: Some(value.timestamp.into()),

            dirent_version: value.dirent_version,
            content_version: value.content_version,

            operation: Some(match value.operation {
                ModifyOperationContent::CreateFileOperation(op) => {
                    proto_types::modify_operation::Operation::CreateFile(op.into())
                }
                ModifyOperationContent::CreateSymlinkOperation(op) => {
                    proto_types::modify_operation::Operation::CreateSymlink(op.into())
                }
                ModifyOperationContent::CreateDirectoryOperation(op) => {
                    proto_types::modify_operation::Operation::CreateDirectory(op.into())
                }
                ModifyOperationContent::RemoveFileOperation(op) => {
                    proto_types::modify_operation::Operation::RemoveFile(op.into())
                }
                ModifyOperationContent::RemoveDirectoryOperation(op) => {
                    proto_types::modify_operation::Operation::RemoveDirectory(op.into())
                }
                ModifyOperationContent::RenameOperation(op) => {
                    proto_types::modify_operation::Operation::Rename(op.into())
                }
                ModifyOperationContent::SetAttributesOperation(op) => {
                    proto_types::modify_operation::Operation::SetAttributes(op.into())
                }
                ModifyOperationContent::WriteOperation(op) => {
                    proto_types::modify_operation::Operation::Write(op.into())
                }
            }),
        }
    }
}

impl From<proto_types::ModifyOperation> for modify_op::ModifyOperation {
    fn from(value: proto_types::ModifyOperation) -> Self {
        modify_op::ModifyOperation {
            id: value.id,
            timestamp: value.timestamp.unwrap().into(),

            dirent_version: value.dirent_version,
            content_version: value.content_version,

            operation: match value.operation.unwrap() {
                Operation::CreateFile(op) => ModifyOperationContent::CreateFileOperation(op.into()),
                Operation::CreateSymlink(op) => {
                    ModifyOperationContent::CreateSymlinkOperation(op.into())
                }
                Operation::CreateDirectory(op) => {
                    ModifyOperationContent::CreateDirectoryOperation(op.into())
                }
                Operation::RemoveFile(op) => ModifyOperationContent::RemoveFileOperation(op.into()),
                Operation::RemoveDirectory(op) => {
                    ModifyOperationContent::RemoveDirectoryOperation(op.into())
                }
                Operation::Rename(op) => ModifyOperationContent::RenameOperation(op.into()),
                Operation::SetAttributes(op) => {
                    ModifyOperationContent::SetAttributesOperation(op.into())
                }
                Operation::Write(op) => ModifyOperationContent::WriteOperation(op.into()),
            },
        }
    }
}

// FileChunkList
impl From<Vec<String>> for FileChunks {
    fn from(value: Vec<String>) -> Self {
        proto_types::FileChunks {
            chunks: value.into(),
        }
    }
}

impl Into<Vec<String>> for FileChunks {
    fn into(self) -> Vec<String> {
        self.chunks.into()
    }
}

// OperationApplyError
impl From<JournalApplyResult> for proto_types::ApplyJournalResponse {
    fn from(value: JournalApplyResult) -> Self {
        match value {
            Ok(data) => {
                let converted_dir_entities: Vec<proto_types::DirEntity> = data
                    .dir_entities
                    .into_iter()
                    .map(|x| x.into())
                    .collect_vec();

                proto_types::ApplyJournalResponse {
                    assigned_ids: data.assigned_ids.into(),
                    dir_entities: converted_dir_entities.into(),
                    error: None,
                }
            }
            Err(err) => {
                let error = match err {
                    OperationApplyError::InvalidJournal => {
                        let data = proto_types::InvalidJournalError {};

                        proto_types::apply_journal_response::Error::InvalidJournal(data)
                    }
                    OperationApplyError::ConflictingFiles(ids) => {
                        let data = proto_types::ConflictingFilesError { ids };

                        proto_types::apply_journal_response::Error::ConflictingFiles(data)
                    }
                    OperationApplyError::MissingBlobs(ids) => {
                        let data = proto_types::MissingBlobsError { ids };

                        proto_types::apply_journal_response::Error::MissingBlobs(data)
                    }
                };

                proto_types::ApplyJournalResponse {
                    assigned_ids: Default::default(),
                    dir_entities: Default::default(),
                    error: Some(error),
                }
            }
        }
    }
}

impl Into<JournalApplyResult> for proto_types::ApplyJournalResponse {
    fn into(self) -> JournalApplyResult {
        if let Some(err) = self.error {
            let converted_error = match err {
                Error::InvalidJournal(_) => OperationApplyError::InvalidJournal,
                Error::ConflictingFiles(data) => {
                    OperationApplyError::ConflictingFiles(data.ids.into())
                }
                Error::MissingBlobs(data) => OperationApplyError::MissingBlobs(data.ids.into()),
            };

            Err(converted_error)
        } else {
            Ok(JournalApplyData {
                assigned_ids: self.assigned_ids.into(),
                dir_entities: self
                    .dir_entities
                    .into_iter()
                    .map(|x| x.into())
                    .collect_vec(),
            })
        }
    }
}
