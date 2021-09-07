use libc::{c_int, EILSEQ, ENODATA, ENOENT, ENOTEMPTY, ENOTRECOVERABLE};
use offs::errors::{OperationError, OperationErrorType};

#[derive(Debug)]
pub enum RemoteFsErrorKind {
    NoEntry,
    InvalidValue,
    Offline,
    TransactionFailure,
    DirectoryNotEmpty,
}

#[derive(Debug)]
pub struct RemoteFsError {
    kind: RemoteFsErrorKind,
}

impl RemoteFsError {
    pub fn new(kind: RemoteFsErrorKind) -> Self {
        Self { kind }
    }

    pub fn to_os_error(&self) -> c_int {
        match self.kind {
            RemoteFsErrorKind::NoEntry => ENOENT,
            RemoteFsErrorKind::InvalidValue => EILSEQ,
            RemoteFsErrorKind::Offline => ENODATA,
            RemoteFsErrorKind::TransactionFailure => ENOTRECOVERABLE,
            RemoteFsErrorKind::DirectoryNotEmpty => ENOTEMPTY,
        }
    }
}

impl From<rusqlite::Error> for RemoteFsError {
    fn from(_: rusqlite::Error) -> Self {
        Self::new(RemoteFsErrorKind::TransactionFailure)
    }
}

impl From<tonic::Status> for RemoteFsError {
    fn from(error: tonic::Status) -> Self {
        let operation_error: OperationError = error.into();
        operation_error.into()
    }
}

impl From<tonic::transport::Error> for RemoteFsError {
    fn from(_: tonic::transport::Error) -> Self {
        Self::new(RemoteFsErrorKind::Offline)
    }
}

impl From<OperationError> for RemoteFsError {
    fn from(error: OperationError) -> Self {
        match error.error_type {
            OperationErrorType::DatabaseError => Self::new(RemoteFsErrorKind::InvalidValue),
            OperationErrorType::DirectoryNotEmpty => {
                Self::new(RemoteFsErrorKind::DirectoryNotEmpty)
            }
            OperationErrorType::ConflictedFile => Self::new(RemoteFsErrorKind::InvalidValue),
            OperationErrorType::InvalidContentVersion => Self::new(RemoteFsErrorKind::InvalidValue),
            OperationErrorType::BlobDoesNotExist => Self::new(RemoteFsErrorKind::InvalidValue),
        }
    }
}
