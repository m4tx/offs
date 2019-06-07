use capnp::serialize_packed;
use time::Timespec;

pub mod filesystem_capnp {
    include!(concat!(env!("OUT_DIR"), "/filesystem_capnp.rs"));

    impl<'a> Into<time::Timespec> for timespec::Reader<'a> {
        fn into(self) -> time::Timespec {
            time::Timespec::new(self.get_sec(), self.get_nsec())
        }
    }
}

pub mod dbus;
pub mod errors;
pub mod modify_op_handler;
pub mod store;
pub mod validators;

pub const PROJ_NAME: &str = env!("CARGO_PKG_NAME");
pub const PROJ_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PROJ_AUTHORS: &str = env!("CARGO_PKG_AUTHORS");

pub const BLOB_SIZE: usize = 64 * 1024;
pub const ROOT_ID: &str = "root";

pub const SQLITE_PAGE_SIZE: i64 = 8192;
pub const SQLITE_CACHE_SIZE: i64 = -32000; // 32MiB

pub fn text_list_to_vec<'a>(reader: ::capnp::text_list::Reader<'a>) -> Vec<String> {
    reader.iter().map(|x| x.unwrap().to_owned()).collect()
}

pub fn vec_to_text_list<'a>(vec: &Vec<String>, mut list: ::capnp::text_list::Builder<'a>) {
    for (i, id) in vec.iter().enumerate() {
        list.set(i as u32, id);
    }
}

pub fn list_list_text_to_vec(
    list: capnp::list_list::Reader<capnp::text_list::Owned>,
) -> Vec<Vec<&str>> {
    let mut result: Vec<Vec<&str>> = Vec::with_capacity(list.len() as usize);

    for i in 0..list.len() {
        let inner = list.get(i).unwrap();
        let mut vec = Vec::new();
        for j in 0..inner.len() {
            vec.push(inner.get(j).unwrap());
        }
        result.push(vec);
    }

    result
}

pub fn serialize_message(
    message: ::capnp::message::Builder<::capnp::message::HeapAllocator>,
) -> Vec<u8> {
    let mut serialized_op = Vec::new();

    serialize_packed::write_message(&mut serialized_op, &message).unwrap();

    serialized_op
}

pub fn now() -> Timespec {
    time::now().to_timespec()
}
