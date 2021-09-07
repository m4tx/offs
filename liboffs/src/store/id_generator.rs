use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use rand::distributions::Standard;
use rand::Rng;

pub trait IdGenerator: Clone {
    fn generate_id(&mut self) -> String;

    fn reset_generator(&mut self) {}
}

#[derive(Clone)]
pub struct RandomHexIdGenerator;

impl RandomHexIdGenerator {
    pub fn new() -> Self {
        Self {}
    }
}

impl IdGenerator for RandomHexIdGenerator {
    fn generate_id(&mut self) -> String {
        hex::encode(
            &rand::thread_rng()
                .sample_iter(&Standard)
                .take(16)
                .collect_vec(),
        )
    }
}

#[derive(Clone)]
pub struct LocalTempIdGenerator {
    pub next_id: Arc<AtomicUsize>,
}

const LOCAL_PREFIX: &str = "temp-";

impl LocalTempIdGenerator {
    pub fn new() -> Self {
        Self {
            next_id: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_nth_id(n: usize) -> String {
        format!("{}{:020}", LOCAL_PREFIX, n)
    }

    pub fn get_n(id: &str) -> usize {
        debug_assert!(Self::is_local_id(id));

        id[LOCAL_PREFIX.len()..]
            .parse()
            .expect(&format!("Invalid temporary ID assigned: {}", id))
    }

    pub fn is_local_id(id: &str) -> bool {
        id.starts_with(LOCAL_PREFIX)
    }
}

impl IdGenerator for LocalTempIdGenerator {
    fn generate_id(&mut self) -> String {
        let result = Self::get_nth_id(self.next_id.load(Ordering::Relaxed));
        self.next_id.fetch_add(1, Ordering::Relaxed);

        result
    }

    fn reset_generator(&mut self) {
        self.next_id.store(0, Ordering::Relaxed);
    }
}
