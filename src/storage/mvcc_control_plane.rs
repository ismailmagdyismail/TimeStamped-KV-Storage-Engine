use std::sync::atomic;

pub struct MVCCControlPlane {
    current_ts: atomic::AtomicU64,
}

impl MVCCControlPlane {
    pub fn new() -> Self {
        let current_ts = atomic::AtomicU64::new(1);
        Self { current_ts }
    }

    pub fn acquire_ts(&mut self) -> u64 {
        let ts = self.current_ts.fetch_add(1, atomic::Ordering::Relaxed);
        ts
    }
}

#[cfg(test)]
mod test {
    use crate::storage::mvcc_control_plane::MVCCControlPlane;

    #[test]
    pub fn test_ts_increment() {
        let mut mvcc_control_plane = MVCCControlPlane::new();
        for i in 1..100 {
            let ts = mvcc_control_plane.acquire_ts();
            assert_eq!(i, ts)
        }
    }
}
