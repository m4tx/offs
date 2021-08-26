use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dbus::blocking::Connection;
use dbus::channel::MatchingReceiver;
use dbus::Error;
use dbus_crossroads::{Crossroads, IfaceBuilder};

use offs::dbus::{ID_PREFIX, IFACE, MOUNT_POINT, OFFLINE_MODE, PATH};

struct InterfaceData {
    mount_point: PathBuf,
    offline_mode: Arc<AtomicBool>,
    should_flush_journal: Arc<AtomicBool>,
}

pub fn run_dbus_server(
    fs_mounted: Arc<AtomicBool>,
    mount_point: PathBuf,
    offline_mode: Arc<AtomicBool>,
    should_flush_journal: Arc<AtomicBool>,
) -> Result<(), Error> {
    let c = Connection::new_session()?;
    let name = format!("{}{}", ID_PREFIX, process::id());
    c.request_name(&name, false, true, false)?;

    let mut cr = Crossroads::new();

    let iface_token = cr.register(IFACE, |b: &mut IfaceBuilder<InterfaceData>| {
        b.property(MOUNT_POINT)
            .get(|_, data| Ok(data.mount_point.to_str().unwrap().to_owned()));

        b.property(OFFLINE_MODE)
            .get(|_, data| Ok(data.offline_mode.load(Ordering::Relaxed)))
            .set(|_, data, enabled| {
                data.offline_mode.store(enabled, Ordering::Relaxed);
                if !enabled {
                    data.should_flush_journal.store(true, Ordering::Relaxed);
                }

                Ok(Some(enabled))
            });
    });

    let data = InterfaceData {
        mount_point,
        offline_mode,
        should_flush_journal,
    };
    cr.insert(PATH, &[iface_token], data);

    c.start_receive(
        dbus::message::MatchRule::new_method_call(),
        Box::new(move |msg, conn| {
            cr.handle_message(msg, conn).unwrap();
            true
        }),
    );

    while fs_mounted.load(Ordering::Relaxed) {
        c.process(Duration::from_millis(1000))?;
    }

    Ok(())
}
