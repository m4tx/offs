use crate::filesystem_capnp::error;
use crate::store::ProtoFill;
use crate::{text_list_to_vec, vec_to_text_list};

#[derive(PartialEq)]
pub enum OperationApplyError {
    None,
    InvalidJournal,
    ConflictingFiles(Vec<String>),
    MissingBlobs(Vec<String>),
}

impl<'a> From<error::Reader<'a>> for OperationApplyError {
    fn from(reader: error::Reader<'a>) -> Self {
        match reader.which().unwrap() {
            error::None(_) => OperationApplyError::None,
            error::InvalidJournal(_) => OperationApplyError::InvalidJournal,
            error::ConflictingFiles(data) => {
                OperationApplyError::ConflictingFiles(text_list_to_vec(data.get_ids().unwrap()))
            }
            error::MissingBlobs(data) => {
                OperationApplyError::MissingBlobs(text_list_to_vec(data.get_ids().unwrap()))
            }
        }
    }
}

impl<'a> ProtoFill<crate::filesystem_capnp::error::Builder<'a>> for OperationApplyError {
    fn fill_proto(&self, mut proto: crate::filesystem_capnp::error::Builder<'a>) {
        match self {
            OperationApplyError::None => {}
            OperationApplyError::InvalidJournal => {
                proto.set_invalid_journal(());
            }
            OperationApplyError::ConflictingFiles(ids) => {
                let err_inner = proto.init_conflicting_files();
                let ids_proto = err_inner.init_ids(ids.len() as u32);
                vec_to_text_list(ids, ids_proto);
            }
            OperationApplyError::MissingBlobs(ids) => {
                let err_inner = proto.init_conflicting_files();
                let ids_proto = err_inner.init_ids(ids.len() as u32);
                vec_to_text_list(ids, ids_proto);
            }
        }
    }
}
