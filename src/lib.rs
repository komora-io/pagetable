use std::mem::MaybeUninit;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

/// A simple 4-level wait-free atomic pagetable. Punches through
/// to the last level and installs any necessary pages along the way.
///
/// Works well for big contiguous metadata.
#[derive(Default)]
pub struct PageTable {
    head: Box<L1>,
}

struct L1 {
    children: [AtomicPtr<L2>; 65536],
}

impl Default for L1 {
    fn default() -> L1 {
        L1 {
            children: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }
}

struct L2 {
    children: [AtomicPtr<L3>; 65536],
}

impl Default for L2 {
    fn default() -> L2 {
        L2 {
            children: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }
}

struct L3 {
    children: [AtomicPtr<L4>; 65536],
}

impl Default for L3 {
    fn default() -> L3 {
        L3 {
            children: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }
}

struct L4 {
    children: [AtomicU64; 65536],
}

impl Default for L4 {
    fn default() -> L4 {
        L4 {
            children: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }
}

impl PageTable {
    pub fn get(&self, key: u64) -> &AtomicU64 {
        let bytes = key.to_be_bytes();
        let k1 = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
        let k2 = usize::from(u16::from_be_bytes([bytes[2], bytes[3]]));
        let k3 = usize::from(u16::from_be_bytes([bytes[4], bytes[5]]));
        let k4 = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));

        let aptr_1: &AtomicPtr<L2> = &self.head.children[k1];
        let mut ptr_1 = aptr_1.load(Ordering::Relaxed);
        if ptr_1.is_null() {
            let c = Box::into_raw(Box::default());
            match aptr_1.compare_exchange_weak(null_mut(), c, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {}
                Err(cur) => {
                    ptr_1 = cur;
                    unsafe {
                        drop(Box::from_raw(c));
                    }
                }
            }
        }
        let l2 = unsafe { &*ptr_1 };

        let aptr_2: &AtomicPtr<L3> = &l2.children[k2];
        let mut ptr_2 = aptr_2.load(Ordering::Relaxed);
        if ptr_2.is_null() {
            let c = Box::into_raw(Box::default());
            match aptr_2.compare_exchange_weak(null_mut(), c, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {}
                Err(cur) => {
                    ptr_2 = cur;
                    unsafe {
                        drop(Box::from_raw(c));
                    }
                }
            }
        }
        let l3 = unsafe { &*ptr_2 };

        let aptr_3: &AtomicPtr<L4> = &l3.children[k3];
        let mut ptr_3 = aptr_3.load(Ordering::Relaxed);
        if ptr_3.is_null() {
            let c = Box::into_raw(Box::default());
            match aptr_3.compare_exchange_weak(null_mut(), c, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {}
                Err(cur) => {
                    ptr_3 = cur;
                    unsafe {
                        drop(Box::from_raw(c));
                    }
                }
            }
        }
        let l4 = unsafe { &*ptr_3 };

        &l4.children[k4]
    }
}
