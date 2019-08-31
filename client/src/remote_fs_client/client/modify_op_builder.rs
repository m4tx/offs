use time::Timespec;

use offs::modify_op::{
    CreateDirectoryOperation, CreateFileOperation, CreateSymlinkOperation, ModifyOperation,
    ModifyOperationContent, RemoveDirectoryOperation, RemoveFileOperation, RenameOperation,
    SetAttributesOperation, WriteOperation,
};
use offs::now;
use offs::store::{DirEntity, FileDev, FileMode, FileType};

pub struct ModifyOpBuilder;

impl ModifyOpBuilder {
    fn create_modify_op(dirent: &DirEntity, content: ModifyOperationContent) -> ModifyOperation {
        ModifyOperation {
            id: dirent.id.clone(),
            timestamp: now(),
            dirent_version: dirent.dirent_version,
            content_version: dirent.content_version,
            operation: content,
        }
    }

    pub fn make_create_file_op(
        parent_dirent: &DirEntity,
        name: &str,
        file_type: FileType,
        perm: FileMode,
        dev: FileDev,
    ) -> ModifyOperation {
        let operation = CreateFileOperation {
            name: name.to_owned(),
            file_type,
            perm,
            dev,
        };
        let content = ModifyOperationContent::CreateFileOperation(operation);

        Self::create_modify_op(parent_dirent, content)
    }

    pub fn make_create_symlink_op(
        parent_dirent: &DirEntity,
        name: &str,
        link: &str,
    ) -> ModifyOperation {
        let operation = CreateSymlinkOperation {
            name: name.to_owned(),
            link: link.to_owned(),
        };
        let content = ModifyOperationContent::CreateSymlinkOperation(operation);

        Self::create_modify_op(parent_dirent, content)
    }

    pub fn make_create_directory_op(
        parent_dirent: &DirEntity,
        name: &str,
        perm: FileMode,
    ) -> ModifyOperation {
        let operation = CreateDirectoryOperation {
            name: name.to_owned(),
            perm,
        };
        let content = ModifyOperationContent::CreateDirectoryOperation(operation);

        Self::create_modify_op(parent_dirent, content)
    }

    pub fn make_remove_file_op(dirent: &DirEntity) -> ModifyOperation {
        let operation = RemoveFileOperation {};
        let content = ModifyOperationContent::RemoveFileOperation(operation);

        Self::create_modify_op(dirent, content)
    }

    pub fn make_remove_directory_op(dirent: &DirEntity) -> ModifyOperation {
        let operation = RemoveDirectoryOperation {};
        let content = ModifyOperationContent::RemoveDirectoryOperation(operation);

        Self::create_modify_op(dirent, content)
    }

    pub fn make_rename_op(dirent: &DirEntity, new_parent: &str, new_name: &str) -> ModifyOperation {
        let operation = RenameOperation {
            new_parent: new_parent.to_owned(),
            new_name: new_name.to_owned(),
        };
        let content = ModifyOperationContent::RenameOperation(operation);

        Self::create_modify_op(dirent, content)
    }

    pub fn make_set_attributes_op(
        dirent: &DirEntity,
        perm: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atim: Option<Timespec>,
        mtim: Option<Timespec>,
    ) -> ModifyOperation {
        let operation = SetAttributesOperation {
            perm,
            uid,
            gid,
            size,
            atim,
            mtim,
        };
        let content = ModifyOperationContent::SetAttributesOperation(operation);

        Self::create_modify_op(dirent, content)
    }

    pub fn make_write_op(dirent: &DirEntity, offset: i64, data: Vec<u8>) -> ModifyOperation {
        let operation = WriteOperation { offset, data };
        let content = ModifyOperationContent::WriteOperation(operation);

        Self::create_modify_op(dirent, content)
    }

    pub fn make_recreate_file_op(parent: &DirEntity, file: &DirEntity) -> ModifyOperation {
        let operation = CreateFileOperation {
            name: file.name.to_owned(),
            file_type: file.stat.file_type,
            perm: file.stat.mode,
            dev: file.stat.dev,
        };
        let content = ModifyOperationContent::CreateFileOperation(operation);

        Self::create_modify_op(parent, content)
    }

    pub fn make_reset_attributes_op(file: &DirEntity) -> ModifyOperation {
        let operation = SetAttributesOperation {
            perm: Some(file.stat.mode),
            uid: Some(file.stat.uid),
            gid: Some(file.stat.gid),
            size: Some(file.stat.size),
            atim: Some(file.stat.atim),
            mtim: Some(file.stat.mtim),
        };
        let content = ModifyOperationContent::SetAttributesOperation(operation);

        Self::create_modify_op(file, content)
    }
}
