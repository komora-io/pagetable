use std::mem::MaybeUninit;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

const FANOUT: usize = 1 << 16;

/// A simple 4-level wait-free atomic pagetable. Punches through
/// to the last level and installs any necessary pages along the way.
///
/// Works well for big contiguous metadata.
///
/// # Examples
///
/// ```rust
/// use std::sync::atomic::Ordering;
///
/// let pt = pagetable::PageTable::default();
///
/// for i in 0..100_000_000 {
///     pt.get(i).fetch_add(1, Ordering::SeqCst);
/// }
///
/// for i in 0..100_000_000 {
///     let value = pt.get(i).load(Ordering::SeqCst);
///     assert_eq!(value, 1);
/// }
/// ```
#[derive(Default)]
pub struct PageTable {
    head: Box<L1>,
    approximate_leaf_count: AtomicU64,
}

struct L1 {
    children: [AtomicPtr<L2>; FANOUT],
}

struct L2 {
    children: [AtomicPtr<L3>; FANOUT],
}

struct L3 {
    children: [AtomicPtr<L4>; FANOUT],
}

struct L4 {
    children: [AtomicU64; FANOUT],
}

fn traverse_or_install<Child: Default>(
    parent: &[AtomicPtr<Child>; FANOUT],
    key: u16,
) -> (&Child, bool) {
    let atomic_ptr: &AtomicPtr<Child> = &parent[key as usize];
    let mut ptr = atomic_ptr.load(Ordering::Acquire);

    let mut installed = false;
    if ptr.is_null() {
        let new_child_ptr = Box::into_raw(Box::default());
        match atomic_ptr.compare_exchange(
            null_mut(),
            new_child_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                ptr = new_child_ptr;
                installed = true;
            }
            Err(cur_ptr) => {
                ptr = cur_ptr;
                unsafe {
                    drop(Box::from_raw(new_child_ptr));
                }
            }
        }
    };

    let child = unsafe { &*ptr };

    (child, installed)
}

impl PageTable {
    /// Get the `AtomicU64` associated with the provided key,
    /// installing all required pages if it does not exist yet.
    /// Defaults to `0`.
    pub fn get(&self, key: u64) -> &AtomicU64 {
        let bytes = key.to_be_bytes();
        let k1 = u16::from_be_bytes([bytes[0], bytes[1]]);
        let k2 = u16::from_be_bytes([bytes[2], bytes[3]]);
        let k3 = u16::from_be_bytes([bytes[4], bytes[5]]);
        let k4 = u16::from_be_bytes([bytes[6], bytes[7]]);

        let l2 = traverse_or_install(&self.head.children, k1).0;
        let l3 = traverse_or_install(&l2.children, k2).0;
        let (l4, installed_leaf) = traverse_or_install(&l3.children, k3);

        if installed_leaf {
            self.approximate_leaf_count.fetch_add(1, Ordering::Relaxed);
        }

        &l4.children[k4 as usize]
    }

    /// A lagging count of the number of instantiated items, stepping
    /// up by 2^16 at a time. Simply multiplies the number of installed
    /// leaf pages by the child page fan-out. Incremented after the page
    /// is installed.
    pub fn approximate_max_child_count(&self) -> u64 {
        self.approximate_leaf_count.load(Ordering::Relaxed) * FANOUT as u64
    }
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
    };
}

impl_drop_children!(L1);
impl_drop_children!(L2);
impl_drop_children!(L3);
// not needed for L4

macro_rules! impl_zeroed_default {
    ($t:ty) => {
        impl Default for $t {
            fn default() -> Self {
                Self {
                    children: unsafe { MaybeUninit::zeroed().assume_init() },
                }
            }
        }
    };
}

impl_zeroed_default!(L1);
impl_zeroed_default!(L2);
impl_zeroed_default!(L3);
impl_zeroed_default!(L4);

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

    pt.get(u64::MAX).fetch_add(1, Ordering::Relaxed);
    let value = pt.get(u64::MAX).load(Ordering::Relaxed);
    assert_eq!(value, 1);
}
