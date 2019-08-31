use itertools::Itertools;
use time::Timespec;

use crate::errors::{JournalApplyData, JournalApplyResult, OperationApplyError};
use crate::modify_op;
use crate::proto::filesystem::FileChunks;
use crate::store as crate_types;
use crate::store::FileMode;

use super::filesystem as proto_types;

// Timespec
impl Into<proto_types::Timespec> for time::Timespec {
    fn into(self) -> proto_types::Timespec {
        let mut timespec = proto_types::Timespec::default();

        timespec.set_sec(self.sec);
        timespec.set_nsec(self.nsec);

        timespec
    }
}

impl Into<time::Timespec> for proto_types::Timespec {
    fn into(self) -> time::Timespec {
        Timespec::new(self.sec, self.nsec)
    }
}

// UInt32Value
impl From<u32> for proto_types::UInt32Value {
    fn from(value: u32) -> Self {
        let mut uint32_value = proto_types::UInt32Value::default();

        uint32_value.set_value(value);

        uint32_value
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
        let mut uint64_value = proto_types::UInt64Value::default();

        uint64_value.set_value(value);

        uint64_value
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
            crate_types::FileType::NamedPipe => proto_types::FileType::NAMED_PIPE,
            crate_types::FileType::CharDevice => proto_types::FileType::CHAR_DEVICE,
            crate_types::FileType::BlockDevice => proto_types::FileType::BLOCK_DEVICE,
            crate_types::FileType::Directory => proto_types::FileType::DIRECTORY,
            crate_types::FileType::RegularFile => proto_types::FileType::REGULAR_FILE,
            crate_types::FileType::Symlink => proto_types::FileType::SYMLINK,
            crate_types::FileType::Socket => proto_types::FileType::SOCKET,
        }
    }
}

impl From<proto_types::FileType> for crate_types::FileType {
    fn from(value: proto_types::FileType) -> Self {
        match value {
            proto_types::FileType::NAMED_PIPE => crate_types::FileType::NamedPipe,
            proto_types::FileType::CHAR_DEVICE => crate_types::FileType::CharDevice,
            proto_types::FileType::BLOCK_DEVICE => crate_types::FileType::BlockDevice,
            proto_types::FileType::DIRECTORY => crate_types::FileType::Directory,
            proto_types::FileType::REGULAR_FILE => crate_types::FileType::RegularFile,
            proto_types::FileType::SYMLINK => crate_types::FileType::Symlink,
            proto_types::FileType::SOCKET => crate_types::FileType::Socket,
        }
    }
}

// Stat
impl From<crate_types::FileStat> for proto_types::Stat {
    fn from(value: crate_types::FileStat) -> Self {
        let mut stat = proto_types::Stat::default();

        stat.set_ino(value.ino);
        stat.set_file_type(value.file_type.into());
        stat.set_perm(value.mode as u32);
        stat.set_nlink(value.nlink);
        stat.set_uid(value.uid);
        stat.set_gid(value.gid);
        stat.set_size(value.size);
        stat.set_blocks(value.blocks);
        stat.set_atim(value.atim.into());
        stat.set_mtim(value.mtim.into());
        stat.set_ctim(value.ctim.into());

        stat
    }
}

impl From<proto_types::Stat> for crate_types::FileStat {
    fn from(value: proto_types::Stat) -> Self {
        crate_types::FileStat {
            ino: value.ino,
            file_type: value.file_type.into(),
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
        let mut dir_entity = proto_types::DirEntity::default();

        dir_entity.set_id(value.id);
        dir_entity.set_parent(value.parent);
        dir_entity.set_name(value.name);

        dir_entity.set_dirent_version(value.dirent_version);
        dir_entity.set_content_version(value.content_version);

        dir_entity.set_stat(value.stat.into());

        dir_entity
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
        let mut create_file_op = proto_types::CreateFileOperation::default();

        create_file_op.set_name(value.name);
        create_file_op.set_file_type(value.file_type.into());
        create_file_op.set_perm(value.perm as u32);
        create_file_op.set_dev(value.dev);

        create_file_op
    }
}

impl From<proto_types::CreateFileOperation> for modify_op::CreateFileOperation {
    fn from(value: proto_types::CreateFileOperation) -> Self {
        modify_op::CreateFileOperation {
            name: value.name,
            file_type: value.file_type.into(),
            perm: value.perm as u16,
            dev: value.dev,
        }
    }
}

// CreateSymlinkOperation
impl From<modify_op::CreateSymlinkOperation> for proto_types::CreateSymlinkOperation {
    fn from(value: modify_op::CreateSymlinkOperation) -> Self {
        let mut create_symlink_op = proto_types::CreateSymlinkOperation::default();

        create_symlink_op.set_name(value.name);
        create_symlink_op.set_link(value.link);

        create_symlink_op
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
        let mut create_directory_op = proto_types::CreateDirectoryOperation::default();

        create_directory_op.set_name(value.name);
        create_directory_op.set_perm(value.perm as u32);

        create_directory_op
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
        let remove_file_op = proto_types::RemoveFileOperation::default();

        remove_file_op
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
        let remove_directory_op = proto_types::RemoveDirectoryOperation::default();

        remove_directory_op
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
        let mut rename_op = proto_types::RenameOperation::default();

        rename_op.set_new_parent(value.new_parent);
        rename_op.set_new_name(value.new_name);

        rename_op
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
        let mut set_attributes_op = proto_types::SetAttributesOperation::default();

        match value.perm {
            None => set_attributes_op.clear_perm(),
            Some(val) => set_attributes_op.set_perm((val as u32).into()),
        }
        match value.uid {
            None => set_attributes_op.clear_uid(),
            Some(val) => set_attributes_op.set_uid(val.into()),
        }
        match value.gid {
            None => set_attributes_op.clear_gid(),
            Some(val) => set_attributes_op.set_gid(val.into()),
        }
        match value.size {
            None => set_attributes_op.clear_size(),
            Some(val) => set_attributes_op.set_size(val.into()),
        }
        match value.atim {
            None => set_attributes_op.clear_atim(),
            Some(val) => set_attributes_op.set_atim(val.into()),
        }
        match value.mtim {
            None => set_attributes_op.clear_mtim(),
            Some(val) => set_attributes_op.set_mtim(val.into()),
        }

        set_attributes_op
    }
}

impl From<proto_types::SetAttributesOperation> for modify_op::SetAttributesOperation {
    fn from(value: proto_types::SetAttributesOperation) -> Self {
        Self {
            perm: value
                .perm
                .into_option()
                .map(|x| Into::<u32>::into(x) as FileMode),
            uid: value.uid.into_option().map(|x| x.into()),
            gid: value.gid.into_option().map(|x| x.into()),
            size: value.size.into_option().map(|x| x.into()),
            atim: value.atim.into_option().map(|x| x.into()),
            mtim: value.mtim.into_option().map(|x| x.into()),
        }
    }
}

// CreateFileOperation
impl From<modify_op::WriteOperation> for proto_types::WriteOperation {
    fn from(value: modify_op::WriteOperation) -> Self {
        let mut write_op = proto_types::WriteOperation::default();

        write_op.set_offset(value.offset);
        write_op.set_data(value.data);

        write_op
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
        let mut modify_op = proto_types::ModifyOperation::default();

        modify_op.set_id(value.id);
        modify_op.set_timestamp(value.timestamp.into());

        modify_op.set_dirent_version(value.dirent_version);
        modify_op.set_content_version(value.content_version);

        match value.operation {
            modify_op::ModifyOperationContent::CreateFileOperation(op) => {
                modify_op.set_create_file(op.into())
            }
            modify_op::ModifyOperationContent::CreateSymlinkOperation(op) => {
                modify_op.set_create_symlink(op.into())
            }
            modify_op::ModifyOperationContent::CreateDirectoryOperation(op) => {
                modify_op.set_create_directory(op.into())
            }

            modify_op::ModifyOperationContent::RemoveFileOperation(op) => {
                modify_op.set_remove_file(op.into())
            }
            modify_op::ModifyOperationContent::RemoveDirectoryOperation(op) => {
                modify_op.set_remove_directory(op.into())
            }

            modify_op::ModifyOperationContent::RenameOperation(op) => {
                modify_op.set_rename(op.into())
            }
            modify_op::ModifyOperationContent::SetAttributesOperation(op) => {
                modify_op.set_set_attributes(op.into())
            }
            modify_op::ModifyOperationContent::WriteOperation(op) => modify_op.set_write(op.into()),
        }

        modify_op
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
                proto_types::ModifyOperation_oneof_operation::create_file(op) => {
                    modify_op::ModifyOperationContent::CreateFileOperation(op.into())
                }
                proto_types::ModifyOperation_oneof_operation::create_symlink(op) => {
                    modify_op::ModifyOperationContent::CreateSymlinkOperation(op.into())
                }
                proto_types::ModifyOperation_oneof_operation::create_directory(op) => {
                    modify_op::ModifyOperationContent::CreateDirectoryOperation(op.into())
                }

                proto_types::ModifyOperation_oneof_operation::remove_file(op) => {
                    modify_op::ModifyOperationContent::RemoveFileOperation(op.into())
                }
                proto_types::ModifyOperation_oneof_operation::remove_directory(op) => {
                    modify_op::ModifyOperationContent::RemoveDirectoryOperation(op.into())
                }

                proto_types::ModifyOperation_oneof_operation::rename(op) => {
                    modify_op::ModifyOperationContent::RenameOperation(op.into())
                }
                proto_types::ModifyOperation_oneof_operation::set_attributes(op) => {
                    modify_op::ModifyOperationContent::SetAttributesOperation(op.into())
                }
                proto_types::ModifyOperation_oneof_operation::write(op) => {
                    modify_op::ModifyOperationContent::WriteOperation(op.into())
                }
            },
        }
    }
}

// FileChunkList
impl From<Vec<String>> for FileChunks {
    fn from(value: Vec<String>) -> Self {
        let mut file_chunks = FileChunks::default();

        file_chunks.set_chunks(value.into());

        file_chunks
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
        let mut response = proto_types::ApplyJournalResponse::default();

        if let Ok(data) = value {
            let converted_dir_entities: Vec<proto_types::DirEntity> = data
                .dir_entities
                .into_iter()
                .map(|x| x.into())
                .collect_vec();

            response.set_assigned_ids(data.assigned_ids.into());
            response.set_dir_entities(converted_dir_entities.into());
        } else if let Err(err) = value {
            match err {
                OperationApplyError::InvalidJournal => {
                    let data = proto_types::InvalidJournalError::default();

                    response.set_invalid_journal(data);
                }
                OperationApplyError::ConflictingFiles(ids) => {
                    let mut data = proto_types::ConflictingFilesError::default();
                    data.set_ids(ids.into());

                    response.set_conflicting_files(data);
                }
                OperationApplyError::MissingBlobs(ids) => {
                    let mut data = proto_types::MissingBlobsError::default();
                    data.set_ids(ids.into());

                    response.set_missing_blobs(data);
                }
            }
        }

        response
    }
}

impl Into<JournalApplyResult> for proto_types::ApplyJournalResponse {
    fn into(self) -> JournalApplyResult {
        if let Some(err) = self.error {
            let converted_error = match err {
                proto_types::ApplyJournalResponse_oneof_error::invalid_journal(_) => {
                    OperationApplyError::InvalidJournal
                }
                proto_types::ApplyJournalResponse_oneof_error::conflicting_files(data) => {
                    OperationApplyError::ConflictingFiles(data.ids.into())
                }
                proto_types::ApplyJournalResponse_oneof_error::missing_blobs(data) => {
                    OperationApplyError::MissingBlobs(data.ids.into())
                }
            };

            Err(converted_error)
        } else {
            Ok(JournalApplyData {
                assigned_ids: self.assigned_ids.into_vec(),
                dir_entities: self
                    .dir_entities
                    .into_iter()
                    .map(|x| x.into())
                    .collect_vec(),
            })
        }
    }
}
