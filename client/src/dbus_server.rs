use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dbus::tree::{Access, Factory};
use dbus::{BusType, Connection, Error, NameFlag};

use offs::dbus::{ID_PREFIX, IFACE, MOUNT_POINT, OFFLINE_MODE, PATH};

pub fn run_dbus_server(
    fs_mounted: Arc<AtomicBool>,
    mount_point: PathBuf,
    offline_mode: Arc<AtomicBool>,
    should_flush_journal: Arc<AtomicBool>,
) -> Result<(), Error> {
    let c = Connection::get_private(BusType::Session)?;
    let name = format!("{}{}", ID_PREFIX, process::id());
    c.register_name(&name, NameFlag::ReplaceExisting as u32)?;
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

    tree.set_registered(&c, true)?;
    c.add_handler(tree);

    while fs_mounted.load(Ordering::Relaxed) {
        c.incoming(1000).next();
    }

    Ok(())
}
