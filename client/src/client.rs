use std::ffi::{CString, OsStr};
use std::net::SocketAddr;
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use nix::mount::umount;

use offs::store::id_generator::LocalTempIdGenerator;
use offs::store::Store;

use crate::remote_fs_client::OffsFilesystem;

use super::dbus_server;

pub fn run_client(
    mount_point: &Path,
    fuse_options: Vec<&OsStr>,
    address: SocketAddr,
    offline_mode: bool,
    store: Store<LocalTempIdGenerator>,
) {
    let fs_mounted = Arc::new(AtomicBool::new(true));
    let offline_mode_val = Arc::new(AtomicBool::new(offline_mode));
    let should_flush_journal = Arc::new(AtomicBool::new(false));

    {
        let fs_mounted_cloned = fs_mounted.clone();
        let mount_point_cloned = mount_point.to_owned();
        let offline_mode_val_cloned = offline_mode_val.clone();
        let should_flush_journal_cloned = should_flush_journal.clone();

        thread::spawn(|| {
            dbus_server::run_dbus_server(
                fs_mounted_cloned,
                mount_point_cloned,
                offline_mode_val_cloned,
                should_flush_journal_cloned,
            )
            .unwrap();
        });
    }

    set_sigterm_handler(mount_point);

    fuse::mount(
        OffsFilesystem::new(address, offline_mode_val, should_flush_journal, store),
        &mount_point,
        &fuse_options,
    )
    .unwrap();

    fs_mounted.store(false, Ordering::Relaxed);
}

fn set_sigterm_handler(mount_point: &Path) {
    let mount_point_cloned = mount_point.to_owned();
    ctrlc::set_handler(move || {
        unmount_fs(&mount_point_cloned);
    })
    .expect("Error setting SIGTERM handler");
}

extern "C" {
    pub fn fuse_unmount_compat22(mountpoint: *const c_char);
}

fn unmount_fs(mount_point: &Path) {
    if let Err(_) = umount(mount_point) {
        let mnt = CString::new(mount_point.as_os_str().as_bytes()).unwrap();
        unsafe {
            fuse_unmount_compat22(mnt.as_ptr());
        }
    }
}
