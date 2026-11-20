use std::sync::atomic;

pub struct MVCCControlPlane {
    current_ts: atomic::AtomicU64,
}

impl MVCCControlPlane {
    pub fn new() -> Self {
        let current_ts = atomic::AtomicU64::new(1);
        Self { current_ts }
    }

    pub fn acquire_ts(&self) -> u64 {
        let ts = self.current_ts.fetch_add(1, atomic::Ordering::Relaxed);
        ts
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread};

    use crate::storage::mvcc_control_plane::MVCCControlPlane;

    #[test]
    pub fn test_ts_increment() {
        let mvcc_control_plane = MVCCControlPlane::new();
        for i in 1..100 {
            let ts = mvcc_control_plane.acquire_ts();
            assert_eq!(i, ts)
        }
    }

    #[test]
    pub fn test_threaded_increment() {
        let mvcc_control_plane = Arc::new(MVCCControlPlane::new());
        let mvcc_control_plane_copy = mvcc_control_plane.clone();
        let control_plane = mvcc_control_plane.clone();
        let iterations = 10;

        let t1 = thread::spawn(move || {
            for _ in 0..iterations {
                mvcc_control_plane.acquire_ts();
            }
        });
        let t2 = thread::spawn(move || {
            for _ in 0..iterations {
                mvcc_control_plane_copy.acquire_ts();
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
        assert_eq!(control_plane.acquire_ts(), iterations * 2 + 1);
    }
}
