use std::time::{Duration, SystemTime};

#[derive(Clone, Copy, Debug)]
pub struct Timespec {
    pub sec: i64,
    pub nsec: u32,
}

impl Timespec {
    pub fn now() -> Self {
        SystemTime::now().into()
    }
}

impl Timespec {
    pub fn new(sec: i64, nsec: u32) -> Self {
        Self { sec, nsec }
    }
}

impl From<SystemTime> for Timespec {
    fn from(system_time: SystemTime) -> Self {
        let duration = system_time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        Timespec::new(duration.as_secs() as i64, duration.subsec_nanos())
    }
}

impl Into<SystemTime> for Timespec {
    fn into(self) -> SystemTime {
        SystemTime::UNIX_EPOCH
            + Duration::from_secs(self.sec as u64)
            + Duration::from_nanos(self.nsec as u64)
    }
}
