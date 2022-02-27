use crate::models::H256;
use std::sync::atomic::AtomicPtr;

pub struct AtomicStatus(AtomicPtr<Status>);

impl AtomicStatus {
    #[inline(always)]
    pub fn new(status: Status) -> Self {
        AtomicStatus(AtomicPtr::new(Box::into_raw(Box::new(status))))
    }

    #[inline(always)]
    pub fn load(&self) -> Status {
        unsafe { *self.0.load(std::sync::atomic::Ordering::Relaxed) }
    }

    #[inline(always)]
    pub fn store(&self, status: Status) {
        self.0.store(
            Box::into_raw(Box::new(status)),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Status {
    pub height: u64,
    pub hash: H256,
    pub total_difficulty: H256,
}

impl PartialEq for Status {
    #[inline(always)]
    fn eq(&self, other: &Status) -> bool {
        self.height == other.height
            && self.hash == other.hash
            && self.total_difficulty == other.total_difficulty
    }
}

impl Status {
    #[inline(always)]
    pub const fn new(height: u64, hash: H256, total_difficulty: H256) -> Self {
        Self {
            height,
            hash,
            total_difficulty,
        }
    }
}

mod tests {
    #[test]
    fn test_atomic_status() {
        use super::*;
        let status = Status::new(1, H256::default(), H256::default());
        let atomic_status = AtomicStatus::new(status);
        assert_eq!(atomic_status.load(), status);
        atomic_status.store(status);
        assert_eq!(atomic_status.load(), status);
        let new_status = Status::new(2, H256::default(), H256::default());
        atomic_status.store(new_status);
        assert_eq!(atomic_status.load(), new_status);
    }
}
