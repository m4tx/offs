use std::result::Result;

use crate::store::DirEntity;

#[derive(PartialEq)]
pub enum OperationApplyError {
    InvalidJournal,
    ConflictingFiles(Vec<String>),
    MissingBlobs(Vec<String>),
}

#[derive(Default)]
pub struct JournalApplyData {
    pub assigned_ids: Vec<String>,
    pub dir_entities: Vec<DirEntity>,
}

pub type JournalApplyResult = Result<JournalApplyData, OperationApplyError>;
