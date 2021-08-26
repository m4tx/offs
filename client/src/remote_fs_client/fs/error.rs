use libc::{c_int, EILSEQ, ENODATA, ENOENT, ENOTRECOVERABLE};

#[derive(Debug)]
pub enum RemoteFsErrorKind {
    NoEntry,
    InvalidValue,
    Offline,
    TransactionFailure,
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
        }
    }
}

impl From<rusqlite::Error> for RemoteFsError {
    fn from(_: rusqlite::Error) -> Self {
        Self::new(RemoteFsErrorKind::TransactionFailure)
    }
}

impl From<tonic::Status> for RemoteFsError {
    fn from(_: tonic::Status) -> Self {
        Self::new(RemoteFsErrorKind::Offline)
    }
}

impl From<tonic::transport::Error> for RemoteFsError {
    fn from(_: tonic::transport::Error) -> Self {
        Self::new(RemoteFsErrorKind::Offline)
    }
}
