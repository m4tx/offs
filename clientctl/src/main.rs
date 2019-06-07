use clap::{App, AppSettings, Arg, SubCommand};

use offs::{PROJ_AUTHORS, PROJ_NAME, PROJ_VERSION};

mod dbus_client;

fn main() {
    let matches = App::new(format!("{} client controller", PROJ_NAME))
        .version(PROJ_VERSION)
        .author(PROJ_AUTHORS)
        .about(format!("{} filesystem client controller", PROJ_NAME).as_ref())
        .setting(AppSettings::VersionlessSubcommands)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("mountpoint")
                .validator(offs::validators::check_is_dir)
                .global(true)
                .short("m")
                .long("mountpoint")
                .value_name("PATH")
                .help("Specifies the client to run the command on"),
        )
        .subcommand(
            SubCommand::with_name("offline-mode").arg(
                Arg::with_name("enable")
                    .required(true)
                    .possible_values(&["on", "off"])
                    .help("Whether to enable or disable offline mode"),
            ),
        )
        .get_matches();

    let mount_point = matches.value_of("mountpoint").unwrap_or("");

    let connection = dbus_client::get_connection().expect("Could not obtain DBus connection");
    let service_id = if mount_point.is_empty() {
        dbus_client::get_only_service(&connection)
    } else {
        dbus_client::get_id_by_mountpoint(&connection, mount_point)
    }
    .expect(&format!("Could not get {} service", PROJ_NAME));

    match matches.subcommand() {
        ("offline-mode", Some(sub_m)) => {
            dbus_client::set_offline_mode(
                &connection,
                &service_id,
                sub_m.value_of("enable").unwrap() == "on",
            )
            .expect("Could not set offline mode");
        }
        _ => unreachable!(),
    }
}
