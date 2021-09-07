use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use tokio::sync::RwLock;

use offs::store::id_generator::LocalTempIdGenerator;
use offs::store::Store;

use crate::remote_fs_client::{FuseOffsFilesystem, OffsFilesystem};

use super::dbus_server;

pub fn run_client(
    mount_point: &Path,
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
            .expect("Could not run D-Bus server");
        });
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let fs = rt.block_on(async move {
        OffsFilesystem::new(address, offline_mode_val, should_flush_journal, store)
            .await
            .expect("Could not create Filesystem instance")
    });
    let fs = Arc::new(RwLock::new(fs));

    let thread_lock = Arc::new((Mutex::new(false), Condvar::new()));
    set_sigterm_handler(thread_lock.clone());

    let session = fuser::Session::new(
        FuseOffsFilesystem::new(fs, rt),
        &mount_point,
        Default::default(),
    )
    .expect("Could not run FUSE session");
    let _background_session = session.spawn().expect("Could not run FUSE session");

    let (lock, cvar) = &*thread_lock;
    let mut interrupted = lock.lock().unwrap();
    while !*interrupted {
        interrupted = cvar.wait(interrupted).unwrap();
    }

    fs_mounted.store(false, Ordering::Relaxed);
}

fn set_sigterm_handler(pair2: Arc<(Mutex<bool>, Condvar)>) {
    ctrlc::set_handler(move || {
        let (lock, cvar) = &*pair2;
        let mut interrupted = lock.lock().unwrap();
        *interrupted = true;
        cvar.notify_one();
    })
    .expect("Error setting SIGTERM handler");
}
