use std::fmt::{Display, Formatter};
use std::result::Result;
use std::str::FromStr;

use bytes::Bytes;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use rusqlite::Error;
use tonic::{Code, Status};

use crate::store::DirEntity;
use crate::ERROR_STATUS_CODE_HEADER_KEY;

#[derive(Clone, Copy, Debug, FromPrimitive, ToPrimitive)]
pub enum OperationErrorType {
    DatabaseError = 0,
    DirectoryNotEmpty,
    ConflictedFile,
    InvalidContentVersion,
    BlobDoesNotExist,
    Offline,
    FileDoesNotExist,
    InvalidUnicode,
}

impl Into<Code> for OperationErrorType {
    fn into(self) -> Code {
        match self {
            OperationErrorType::DatabaseError => Code::FailedPrecondition,
            OperationErrorType::DirectoryNotEmpty => Code::FailedPrecondition,
            OperationErrorType::ConflictedFile => Code::AlreadyExists,
            OperationErrorType::InvalidContentVersion => Code::FailedPrecondition,
            OperationErrorType::BlobDoesNotExist => Code::InvalidArgument,
            OperationErrorType::Offline => Code::Unavailable,
            OperationErrorType::FileDoesNotExist => Code::InvalidArgument,
            OperationErrorType::InvalidUnicode => Code::InvalidArgument,
        }
    }
}

#[derive(Clone, Debug)]
pub struct OperationError {
    pub error_type: OperationErrorType,
    pub message: String,
    pub details: Bytes,
}

impl OperationError {
    pub fn new(error_type: OperationErrorType, message: String) -> Self {
        Self {
            error_type,
            message,
            details: Default::default(),
        }
    }

    pub fn with_details(error_type: OperationErrorType, message: String, details: Bytes) -> Self {
        Self {
            error_type,
            message,
            details,
        }
    }

    pub fn directory_not_empty() -> Self {
        Self {
            error_type: OperationErrorType::DirectoryNotEmpty,
            message: "Directory not empty".to_owned(),
            details: Default::default(),
        }
    }

    pub fn conflicted_file(id: String) -> Self {
        Self {
            error_type: OperationErrorType::ConflictedFile,
            message: format!("Conflicted file: {}", id),
            details: Bytes::from(id),
        }
    }

    pub fn invalid_content_version() -> Self {
        Self {
            error_type: OperationErrorType::InvalidContentVersion,
            message: "Invalid content version".to_owned(),
            details: Default::default(),
        }
    }

    pub fn blob_does_not_exist(id: &str) -> Self {
        Self {
            error_type: OperationErrorType::BlobDoesNotExist,
            message: format!("Blob {} does not exist", id),
            details: Default::default(),
        }
    }

    pub fn offline(message: &str) -> Self {
        Self::new(
            OperationErrorType::Offline,
            format!("Connection error: {}", message),
        )
    }

    pub fn file_does_not_exist(id: &str) -> Self {
        Self::new(
            OperationErrorType::FileDoesNotExist,
            format!("File {} does not exist", id),
        )
    }

    pub fn invalid_unicode() -> Self {
        Self::new(
            OperationErrorType::InvalidUnicode,
            "Invalid unicode string".to_owned(),
        )
    }
}

impl Display for OperationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Operation Error: code {:?}; message: {}",
            self.error_type, self.message
        )
    }
}

impl std::error::Error for OperationError {}

impl From<rusqlite::Error> for OperationError {
    fn from(error: Error) -> Self {
        Self::new(OperationErrorType::DatabaseError, error.to_string())
    }
}

impl From<OperationError> for tonic::Status {
    fn from(error: OperationError) -> Self {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.append(
            ERROR_STATUS_CODE_HEADER_KEY,
            tonic::metadata::MetadataValue::from(error.error_type as u64),
        );

        Self::with_details_and_metadata(
            error.error_type.into(),
            error.message,
            error.details,
            metadata,
        )
    }
}

impl From<tonic::Status> for OperationError {
    fn from(status: Status) -> Self {
        Self::with_details(
            FromPrimitive::from_u64(
                u64::from_str(
                    status
                        .metadata()
                        .get(ERROR_STATUS_CODE_HEADER_KEY)
                        .unwrap()
                        .to_str()
                        .unwrap(),
                )
                .unwrap(),
            )
            .unwrap(),
            status.message().to_owned(),
            Bytes::copy_from_slice(status.details()),
        )
    }
}

impl From<tonic::transport::Error> for OperationError {
    fn from(error: tonic::transport::Error) -> Self {
        OperationError::offline(&error.to_string())
    }
}

pub type OperationResult<T> = Result<T, OperationError>;

#[derive(PartialEq)]
pub enum JournalApplyError {
    InvalidJournal,
    ConflictingFiles(Vec<String>),
    MissingBlobs(Vec<String>),
}

#[derive(Default)]
pub struct JournalApplyData {
    pub assigned_ids: Vec<String>,
    pub dir_entities: Vec<DirEntity>,
}

pub type JournalApplyResult = Result<JournalApplyData, JournalApplyError>;
