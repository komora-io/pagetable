use std::mem::MaybeUninit;
use std::ptr::null_mut;
use std::sync::{
    atomic::{
        AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicPtr, AtomicU16,
        AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
    },
    Arc,
};

const FANOUT: usize = 1 << 16;

/// This trait is true for numerical values that are valid
/// even if they are initialized to zero bytes.
pub trait Zeroable {}

impl Zeroable for AtomicBool {}
impl Zeroable for AtomicI8 {}
impl Zeroable for AtomicI16 {}
impl Zeroable for AtomicI32 {}
impl Zeroable for AtomicI64 {}
impl Zeroable for AtomicIsize {}
impl Zeroable for AtomicU8 {}
impl Zeroable for AtomicU16 {}
impl Zeroable for AtomicU32 {}
impl Zeroable for AtomicU64 {}
impl Zeroable for AtomicUsize {}
impl<T> Zeroable for AtomicPtr<T> {}

/// A simple 4-level wait-free atomic pagetable. Punches through
/// to the last level and installs any necessary pages along the way.
///
/// Works well for big contiguous metadata.
///
/// # Examples
///
/// ```rust
/// use std::sync::atomic::{AtomicU64, Ordering};
///
/// let pt = pagetable::PageTable::<AtomicU64>::default();
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
pub struct PageTable<T: Zeroable> {
    head: Arc<L1<T>>,
}

impl<T: Zeroable> Clone for PageTable<T> {
    fn clone(&self) -> Self {
        PageTable {
            head: self.head.clone(),
        }
    }
}

struct L1<T> {
    children: [AtomicPtr<L2<T>>; FANOUT],
}

struct L2<T> {
    children: [AtomicPtr<L3<T>>; FANOUT],
}

struct L3<T> {
    children: [AtomicPtr<L4<T>>; FANOUT],
}

struct L4<T> {
    children: [T; FANOUT],
}

// Punches-through an atomic pointer and either dereferences it or attempts to create it.
// This is conveniently wait-free due to the bounded maximum amount of work that may happen
// in this process.
fn traverse_or_install<Child: Default>(parent: &[AtomicPtr<Child>; FANOUT], key: u16) -> &Child {
    let atomic_ptr: &AtomicPtr<Child> = &parent[key as usize];
    let mut ptr = atomic_ptr.load(Ordering::Acquire);

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
            }
            Err(cur_ptr) => {
                ptr = cur_ptr;
                unsafe {
                    drop(Box::from_raw(new_child_ptr));
                }
            }
        }
    };

    unsafe { &*ptr }
}

impl<T: Zeroable> PageTable<T> {
    /// Get the `AtomicU64` associated with the provided key,
    /// installing all required pages if it does not exist yet.
    /// Defaults to `0`.
    pub fn get(&self, key: u64) -> &T {
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

macro_rules! impl_drop_children {
    ($t:ty) => {
        impl<T> Drop for $t {
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

impl_drop_children!(L1<T>);
impl_drop_children!(L2<T>);
impl_drop_children!(L3<T>);
// not needed for L4

macro_rules! impl_zeroed_default {
    ($t:ty) => {
        impl<T> Default for $t {
            fn default() -> Self {
                Self {
                    children: unsafe { MaybeUninit::zeroed().assume_init() },
                }
            }
        }
    };
}

impl_zeroed_default!(L1<T>);
impl_zeroed_default!(L2<T>);
impl_zeroed_default!(L3<T>);
impl_zeroed_default!(L4<T>);

#[test]
fn smoke() {
    #[cfg(miri)]
    const N: u64 = 1;

    #[cfg(not(miri))]
    const N: u64 = 100_000_000;

    let pt = PageTable::<AtomicU64>::default();

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
