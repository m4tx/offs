use libc::{c_int, EBADFD, EEXIST, EINVAL, ENOENT, ENOTEMPTY, ENOTRECOVERABLE, ETIMEDOUT};

use offs::errors::{OperationError, OperationErrorType};

pub fn to_os_error(operation_error: &OperationError) -> c_int {
    match operation_error.error_type {
        OperationErrorType::DatabaseError => ENOTRECOVERABLE,
        OperationErrorType::DirectoryNotEmpty => ENOTEMPTY,
        OperationErrorType::ConflictedFile => EEXIST,
        OperationErrorType::InvalidContentVersion => EBADFD,
        OperationErrorType::BlobDoesNotExist => ENOENT,
        OperationErrorType::Offline => ETIMEDOUT,
        OperationErrorType::FileDoesNotExist => ENOENT,
        OperationErrorType::InvalidUnicode => EINVAL,
    }
}
