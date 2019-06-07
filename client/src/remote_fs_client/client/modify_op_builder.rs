use time::Timespec;

use offs::filesystem_capnp::modify_operation as ops;
use offs::now;
use offs::store::{DirEntity, FileDev, FileMode, FileType, ProtoFill};

pub struct ModifyOpBuilder;

impl ModifyOpBuilder {
    fn init_modify_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        dirent: &DirEntity,
    ) -> ops::Builder<'a> {
        let mut op = message.init_root::<ops::Builder>();

        op.reborrow().set_id(&dirent.id);
        let current_time = now();
        let mut timestamp = op.reborrow().init_timestamp();
        timestamp.set_sec(current_time.sec);
        timestamp.set_nsec(current_time.nsec);
        op.reborrow().set_dirent_version(dirent.dirent_version);
        op.reborrow().set_content_version(dirent.content_version);

        op
    }

    pub fn make_create_file_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        parent_dirent: &DirEntity,
        name: &str,
        file_type: FileType,
        mode: FileMode,
        dev: FileDev,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, parent_dirent);

        let mut params = op.reborrow().init_create_file();
        params.set_name(name);
        params.set_file_type(file_type.into());
        params.set_mode(mode);
        params.set_dev(dev);

        op
    }

    pub fn make_create_symlink_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        parent_dirent: &DirEntity,
        name: &str,
        link: &str,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, parent_dirent);

        let mut params = op.reborrow().init_create_symlink();
        params.set_name(name);
        params.set_link(link);

        op
    }

    pub fn make_create_directory_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        parent_dirent: &DirEntity,
        name: &str,
        mode: FileMode,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, parent_dirent);

        let mut params = op.reborrow().init_create_directory();
        params.set_name(name);
        params.set_mode(mode);

        op
    }

    pub fn make_remove_file_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        dirent: &DirEntity,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, dirent);

        op.reborrow().init_remove_file();

        op
    }

    pub fn make_remove_directory_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        dirent: &DirEntity,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, dirent);

        op.reborrow().init_remove_file();

        op
    }

    pub fn make_rename_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        dirent: &DirEntity,
        new_parent: &str,
        new_name: &str,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, dirent);

        let mut params = op.reborrow().init_rename();
        params.set_new_parent(new_parent);
        params.set_new_name(new_name);

        op
    }

    pub fn make_set_attributes_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        dirent: &DirEntity,
        mode: Option<FileMode>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, dirent);

        let mut params = op.reborrow().init_set_attributes();
        let mut to_save = params.reborrow().init_to_save();
        if mode.is_some() {
            to_save.set_mode(mode.unwrap());
        }
        if uid.is_some() {
            to_save.set_uid(uid.unwrap());
        }
        if gid.is_some() {
            to_save.set_gid(gid.unwrap());
        }
        if size.is_some() {
            to_save.set_size(size.unwrap());
        }
        if atime.is_some() {
            let mut atim = to_save.reborrow().init_atim();
            atim.set_sec(atime.unwrap().sec);
            atim.set_nsec(atime.unwrap().nsec);
        }
        if mtime.is_some() {
            let mut mtim = to_save.reborrow().init_mtim();
            mtim.set_sec(mtime.unwrap().sec);
            mtim.set_nsec(mtime.unwrap().nsec);
        }

        op
    }

    pub fn make_write_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        dirent: &DirEntity,
        offset: i64,
        data: &[u8],
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, dirent);

        let mut params = op.reborrow().init_write();
        params.set_offset(offset);
        params.set_data(data);

        op
    }

    pub fn make_recreate_file_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        parent: &DirEntity,
        file: &DirEntity,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, parent);

        let mut params = op.reborrow().init_create_file();
        params.set_name(&file.name);
        params.set_mode(file.stat.mode);
        params.set_dev(file.stat.dev);

        op
    }

    pub fn make_reset_attributes_op<'a>(
        message: &'a mut ::capnp::message::Builder<::capnp::message::HeapAllocator>,
        file: &DirEntity,
    ) -> ops::Builder<'a> {
        let mut op = Self::init_modify_op(message, file);

        let mut params = op.reborrow().init_set_attributes();
        let mut to_save = params.reborrow().init_to_save();
        to_save.set_mode(file.stat.mode);
        to_save.set_uid(file.stat.uid);
        to_save.set_gid(file.stat.gid);
        to_save.set_size(file.stat.size as u64);

        let atim = to_save.reborrow().init_atim();
        file.stat.atim.fill_proto(atim);

        let mtim = to_save.reborrow().init_mtim();
        file.stat.mtim.fill_proto(mtim);

        op
    }
}
