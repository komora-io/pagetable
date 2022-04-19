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

macro_rules! impl_drop_children {
    ($t:ty) => {
        impl Drop for $t {
            fn drop(&mut self) {
                for child in &self.children {
                    let ptr = child.load(Ordering::Acquire);
                    if !ptr.is_null() {
                        unsafe {
                            drop(Box::from_raw(ptr));
                        }
                    }
                }
            }
        }
    }
}

impl_drop_children!(L1);
impl_drop_children!(L2);
impl_drop_children!(L3);

const FANOUT: usize = u16::MAX as usize;

struct L1 {
    children: [AtomicPtr<L2>; FANOUT],
}

impl Default for L1 {
    fn default() -> L1 {
        L1 {
            children: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }
}

struct L2 {
    children: [AtomicPtr<L3>; FANOUT],
}

impl Default for L2 {
    fn default() -> L2 {
        L2 {
            children: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }
}

struct L3 {
    children: [AtomicPtr<L4>; FANOUT],
}

impl Default for L3 {
    fn default() -> L3 {
        L3 {
            children: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }
}

struct L4 {
    children: [AtomicU64; FANOUT],
}

impl Default for L4 {
    fn default() -> L4 {
        L4 {
            children: unsafe { MaybeUninit::zeroed().assume_init() },
        }
    }
}

fn traverse_or_install<Child: Default>(parent: &[AtomicPtr<Child>; FANOUT], key: u16) -> &Child {
    let aptr_1: &AtomicPtr<Child> = &parent[key as usize];
    let mut ptr_1 = aptr_1.load(Ordering::Acquire);

    if ptr_1.is_null() {
        let c = Box::into_raw(Box::default());
        match aptr_1.compare_exchange_weak(null_mut(), c, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                ptr_1 = c;
            }
            Err(cur) => {
                ptr_1 = cur;
                unsafe {
                    drop(Box::from_raw(c));
                }
            }
        }
    }
    unsafe { &*ptr_1 }
}

impl PageTable {
    pub fn get(&self, key: u64) -> &AtomicU64 {
        let bytes = key.to_be_bytes();
        let k1 = u16::from_be_bytes([bytes[0], bytes[1]]);
        let k2 = u16::from_be_bytes([bytes[2], bytes[3]]);
        let k3 = u16::from_be_bytes([bytes[4], bytes[5]]);
        let k4 = u16::from_be_bytes([bytes[6], bytes[7]]);

        let l2 = traverse_or_install(&self.head.children, k1);
        let l3 = traverse_or_install(&l2.children, k2);
        let l4 = traverse_or_install(&l3.children, k3);
        &l4.children[k4 as usize]
    }
}

#[test]
fn smoke() {
    #[cfg(miri)]
    const N: u64 = 1;

    #[cfg(not(miri))]
    const N: u64 = 100_000_000;

    let pt = PageTable::default();

    for i in 0..N {
        pt.get(i).fetch_add(1, Ordering::Relaxed);
    }

    for i in 0..N {
        let value = pt.get(i).load(Ordering::Relaxed);
        assert_eq!(value, 1);
    }
}
