use std::fmt::{Debug, Error, Formatter};
use std::time::Duration;

use dbus::arg::{Array, Variant};
use dbus::blocking::stdintf::org_freedesktop_dbus::Properties;
use dbus::blocking::{BlockingSender, Connection};
use dbus::Message;

use offs::dbus::{ID_PREFIX, IFACE, MOUNT_POINT, OFFLINE_MODE, PATH};
use offs::PROJ_NAME;

pub struct DBusClientError {
    pub message: String,
    pub mount_points: Vec<String>,
}

type DBusClientResult<T> = Result<T, DBusClientError>;

impl DBusClientError {
    fn with_message(message: String) -> Self {
        Self {
            message,
            mount_points: Default::default(),
        }
    }

    fn with_message_and_mp_list(message: String, mount_points: Vec<String>) -> Self {
        Self {
            message,
            mount_points,
        }
    }

    fn none_error() -> Self {
        DBusClientError::with_message(format!("Could not obtain running {} services", PROJ_NAME))
    }
}

impl Debug for DBusClientError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_str(&self.message)?;

        if !self.mount_points.is_empty() {
            f.write_str("\n\nAvailable mount points:")?;

            for mount_point in &self.mount_points {
                f.write_str(&format!("* {}", mount_point))?;
            }
        }

        Ok(())
    }
}

impl From<dbus::Error> for DBusClientError {
    fn from(error: dbus::Error) -> Self {
        DBusClientError::with_message(error.to_string())
    }
}

impl From<String> for DBusClientError {
    fn from(message: String) -> Self {
        DBusClientError::with_message(message)
    }
}

pub fn get_connection() -> DBusClientResult<Connection> {
    Ok(Connection::new_session()?)
}

fn get_services(connection: &Connection) -> Result<Vec<String>, DBusClientError> {
    let m = Message::new_method_call(
        "org.freedesktop.DBus",
        "/",
        "org.freedesktop.DBus",
        "ListNames",
    )?;
    let r = connection.send_with_reply_and_block(m, Duration::from_millis(1000))?;
    let arr: Array<&str, _> = r.get1().ok_or(DBusClientError::none_error())?;

    Ok(arr
        .filter(|x| x.starts_with(ID_PREFIX))
        .map(|x| x.to_owned())
        .collect())
}

fn get_mount_points(
    connection: &Connection,
    services: Vec<String>,
) -> Result<Vec<String>, DBusClientError> {
    let mut vec = Vec::new();

    for id in services {
        vec.push(get_mount_point(connection, &id)?);
    }

    Ok(vec)
}

fn get_mount_point(connection: &Connection, service_id: &str) -> Result<String, DBusClientError> {
    let p = connection.with_proxy(service_id, PATH, Duration::from_millis(1000));
    Ok(p.get(IFACE, MOUNT_POINT)?)
}

pub fn get_id_by_mountpoint(
    connection: &Connection,
    mount_point: &str,
) -> Result<String, DBusClientError> {
    let services = get_services(&connection)?;

    let mut mount_points = Vec::new();

    for service in services {
        let mp = get_mount_point(&connection, &service)?;

        if mp == mount_point {
            return Ok(service);
        }

        mount_points.push(mp);
    }

    Err(DBusClientError::with_message_and_mp_list(
        format!(
            "{} client running for the mount point specified was not found",
            PROJ_NAME
        ),
        mount_points,
    ))
}

pub fn get_only_service(connection: &Connection) -> Result<String, DBusClientError> {
    let mut services = get_services(&connection)?;

    if services.len() == 1 {
        Ok(services.pop().ok_or(DBusClientError::none_error())?)
    } else if services.is_empty() {
        Err(DBusClientError::with_message(format!(
            "No {} clients found",
            PROJ_NAME
        )))
    } else {
        Err(DBusClientError::with_message_and_mp_list(
            format!("More than one {} client found", PROJ_NAME),
            get_mount_points(&connection, services)?,
        ))
    }
}

pub fn set_offline_mode(
    connection: &Connection,
    service_id: &str,
    enabled: bool,
) -> Result<(), DBusClientError> {
    let p = connection.with_proxy(service_id, PATH, Duration::from_millis(2000));
    p.set(IFACE, OFFLINE_MODE, Variant(enabled))?;

    Ok(())
}
