use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dbus::blocking::LocalConnection;
use dbus_tree::{Access, Factory};
use dbus::Error;

use offs::dbus::{ID_PREFIX, IFACE, MOUNT_POINT, OFFLINE_MODE, PATH};

pub fn run_dbus_server(
    fs_mounted: Arc<AtomicBool>,
    mount_point: PathBuf,
    offline_mode: Arc<AtomicBool>,
    should_flush_journal: Arc<AtomicBool>,
) -> Result<(), Error> {
    let c = LocalConnection::new_session()?;
    let name = format!("{}{}", ID_PREFIX, process::id());
    c.request_name(&name, false, true, false)?;
    let f = Factory::new_fn::<()>();

    let offline_mode_write = offline_mode.clone();
    let tree = f.tree(()).add(
        f.object_path(PATH, ()).introspectable().add(
            f.interface(IFACE, ())
                .add_p(
                    f.property::<&str, _>(MOUNT_POINT, ())
                        .access(Access::Read)
                        .on_get(move |i, _| {
                            i.append(mount_point.to_str().unwrap());
                            Ok(())
                        }),
                )
                .add_p(
                    f.property::<bool, _>(OFFLINE_MODE, ())
                        .access(Access::ReadWrite)
                        .on_get(move |i, _| {
                            i.append(offline_mode.load(Ordering::Relaxed));

                            Ok(())
                        })
                        .on_set(move |i, _| {
                            let enabled: bool = i.read()?;
                            offline_mode_write.store(enabled, Ordering::Relaxed);
                            if !enabled {
                                should_flush_journal.store(true, Ordering::Relaxed);
                            }

                            Ok(())
                        }),
                ),
        ),
    );

    tree.start_receive(&c);
    while fs_mounted.load(Ordering::Relaxed) {
        c.process(Duration::from_millis(1000))?;
    }

    Ok(())
}
