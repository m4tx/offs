use crate::timespec::Timespec;

pub mod dbus;
pub mod errors;
pub mod modify_op;
pub mod modify_op_handler;
pub mod proto;
pub mod store;
pub mod timespec;
pub mod validators;

pub const PROJ_NAME: &str = env!("CARGO_PKG_NAME");
pub const PROJ_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PROJ_AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

pub const BLOB_SIZE: usize = 64 * 1024;
pub const ROOT_ID: &str = "root";

pub const SQLITE_PAGE_SIZE: i64 = 8192;
pub const SQLITE_CACHE_SIZE: i64 = -32000; // 32MiB

pub const ERROR_STATUS_CODE_HEADER_KEY: &str = "offs-status-code";

pub fn now() -> Timespec {
    Timespec::now()
}
