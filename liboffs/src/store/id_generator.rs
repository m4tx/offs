use itertools::Itertools;
use rand::distributions::Standard;
use rand::Rng;

pub trait IdGenerator {
    fn generate_id(&mut self) -> String;

    fn reset_generator(&mut self) {}
}

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

pub struct LocalTempIdGenerator {
    pub next_id: usize,
}

impl LocalTempIdGenerator {
    pub fn new() -> Self {
        Self { next_id: 0 }
    }

    pub fn get_nth_id(n: usize) -> String {
        format!("temp-{:020}", n)
    }

    pub fn get_n(id: &str) -> usize {
        debug_assert!(Self::is_local_id(id));

        id[5..].parse().unwrap()
    }

    pub fn is_local_id(id: &str) -> bool {
        id.starts_with("temp-")
    }
}

impl IdGenerator for LocalTempIdGenerator {
    fn generate_id(&mut self) -> String {
        let result = Self::get_nth_id(self.next_id);
        self.next_id += 1;

        result
    }

    fn reset_generator(&mut self) {
        self.next_id = 0;
    }
}
