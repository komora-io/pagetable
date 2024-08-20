#![deny(clippy::large_stack_arrays, clippy::large_types_passed_by_value)]
use std::alloc::{alloc_zeroed, Layout};
use std::ptr::null_mut;
use std::sync::{
    atomic::{
        AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicPtr, AtomicU16,
        AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
    },
    Arc,
};

const FANOUT: usize = 1 << 16;

fn _impl_send_sync() {
    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    is_send::<PageTable<AtomicU8>>();
    is_sync::<PageTable<AtomicU8>>();
}

/// This trait is true for numerical values that are valid
/// even if they are initialized to zero bytes.
pub trait Zeroable: Sync {}

impl<T: Zeroable, const LEN: usize> Zeroable for [T; LEN] {}

impl<A: Zeroable, B: Zeroable> Zeroable for (A, B) {}
impl<A: Zeroable, B: Zeroable, C: Zeroable> Zeroable for (A, B, C) {}
impl<A: Zeroable, B: Zeroable, C: Zeroable, D: Zeroable> Zeroable for (A, B, C, D) {}
impl<A: Zeroable, B: Zeroable, C: Zeroable, D: Zeroable, E: Zeroable> Zeroable for (A, B, C, D, E) {}
impl<A: Zeroable, B: Zeroable, C: Zeroable, D: Zeroable, E: Zeroable, F: Zeroable> Zeroable
    for (A, B, C, D, E, F)
{
}

macro_rules! impl_zeroable {
    ($($t:ty),+) => {
        $(
            impl Zeroable for $t {}
        )+
    };
}

impl_zeroable!(
    AtomicBool,
    AtomicI8,
    AtomicI16,
    AtomicI32,
    AtomicI64,
    AtomicIsize,
    AtomicU8,
    AtomicU16,
    AtomicU32,
    AtomicU64,
    AtomicUsize
);

impl<T> Zeroable for AtomicPtr<T> {}

/// A simple 4-level wait-free atomic pagetable. Punches through
/// to the last level and installs any necessary pages along the way.
///
/// Works well for big contiguous metadata. Defaults all values to
/// zeroed bits, so this only works with atomic values, as it's not
/// distinguishable whether a value was ever set to 0 or simply never
/// initialized.
///
/// Warning: don't use this for sparse keyspaces, as any time a key
/// in a 2^16 range is touched for the first time, the zeroed slabs
/// of values and parents (each with 2^16 children) will be created. For
/// 64-bit values (and parent levels that point down) each slab is
/// `8 * 2^16` bytes of memory, or 512kb. It is expected that this
/// page table will be used for very hot global shared metadata.
///
/// There are some simple optimizations for when only the lowest bits
/// in the `u64` are used, which skips allocation and traversal of
/// parent levels. For tables where only keys `0..2^16` are used,
/// traversals can jump directly to the leaf slab, and avoid allocations.
///
/// The same principle is applied for avoiding levels when only the bottom
/// 32 or 48 bits are used for keys.
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
pub struct PageTable<T: Zeroable> {
    inner: Arc<PageTableInner<T>>,
}

impl<T: Zeroable> Default for PageTable<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Zeroable> Clone for PageTable<T> {
    fn clone(&self) -> Self {
        PageTable {
            inner: self.inner.clone(),
        }
    }
}

pub struct PageTableInner<T: Zeroable> {
    // the "root" entry point for anything that can't use shortcuts
    l1: AtomicPtr<L1<T>>,
    // third-fastest shortcut for the first 2^48 keys
    l2: AtomicPtr<L2<T>>,
    // second-fastest shortcut for the first 2^32 keys
    l3: AtomicPtr<L3<T>>,
    // fastest shortcut for the first 2^16 keys to avoid pointer chasing
    l4: AtomicPtr<L4<T>>,
}

impl<T: Zeroable> Default for PageTableInner<T> {
    fn default() -> Self {
        PageTableInner {
            l1: AtomicPtr::new(null_mut()),
            l2: AtomicPtr::new(null_mut()),
            l3: AtomicPtr::new(null_mut()),
            l4: AtomicPtr::new(null_mut()),
        }
    }
}

impl<T: Zeroable> Drop for PageTableInner<T> {
    fn drop(&mut self) {
        let l1 = self.l1.load(Ordering::Acquire);
        if !l1.is_null() {
            unsafe { drop(Box::from_raw(l1)) }
        }
        let l2 = self.l2.load(Ordering::Acquire);
        if !l2.is_null() {
            unsafe { drop(Box::from_raw(l2)) }
        }
        let l3 = self.l3.load(Ordering::Acquire);
        if !l3.is_null() {
            unsafe { drop(Box::from_raw(l3)) }
        }
        let l4 = self.l4.load(Ordering::Acquire);
        if !l4.is_null() {
            unsafe { drop(Box::from_raw(l4)) }
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

#[inline]
fn punch_through<Child: Zeroable>(atomic_ptr: &AtomicPtr<Child>) -> &Child {
    let mut ptr = atomic_ptr.load(Ordering::Acquire);

    if ptr.is_null() {
        let layout = Layout::new::<Child>();
        let new_child_ptr = unsafe { alloc_zeroed(layout) as *mut Child };
        assert!(!new_child_ptr.is_null());

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

// Punches-through an atomic pointer and either dereferences it or attempts to create it.
// This is conveniently wait-free due to the bounded maximum amount of work that may happen
// in this process.
#[inline]
fn traverse_or_install<Child: Zeroable>(parent: &[AtomicPtr<Child>; FANOUT], key: u16) -> &Child {
    let atomic_ptr: &AtomicPtr<Child> = &parent[key as usize];
    punch_through(atomic_ptr)
}

impl<T: Zeroable> PageTable<T> {
    /// Create a new `PageTable`.
    pub fn new() -> Self {
        PageTable {
            inner: Arc::default(),
        }
    }

    /// Get the `AtomicU64` associated with the provided key,
    /// installing all required pages if it does not exist yet.
    /// Defaults to `0`.
    pub fn get(&self, key: u64) -> &T {
        let bytes = key.to_be_bytes();
        let k1 = u16::from_be_bytes([bytes[0], bytes[1]]);
        let k2 = u16::from_be_bytes([bytes[2], bytes[3]]);
        let k3 = u16::from_be_bytes([bytes[4], bytes[5]]);
        let k4 = u16::from_be_bytes([bytes[6], bytes[7]]);

        let direct_4 = k1 | k2 | k3 == 0;
        let direct_3 = k1 | k2 == 0;
        let direct_2 = k1 == 0;

        let l4 = if direct_4 {
            punch_through(&self.inner.l4)
        } else if direct_3 {
            let l3 = punch_through(&self.inner.l3);
            traverse_or_install(&l3.children, k3)
        } else if direct_2 {
            let l2 = punch_through(&self.inner.l2);
            let l3 = traverse_or_install(&l2.children, k2);
            traverse_or_install(&l3.children, k3)
        } else {
            let l1 = punch_through(&self.inner.l1);
            let l2 = traverse_or_install(&l1.children, k1);
            let l3 = traverse_or_install(&l2.children, k2);
            traverse_or_install(&l3.children, k3)
        };

        &l4.children[k4 as usize]
    }
}

impl<T: Zeroable, I: Into<u64>> std::ops::Index<I> for PageTable<T> {
    type Output = T;

    fn index(&self, index: I) -> &Self::Output {
        self.get(index.into())
    }
}

impl<T: Zeroable> Zeroable for L1<T> {}
impl<T: Zeroable> Zeroable for L2<T> {}
impl<T: Zeroable> Zeroable for L3<T> {}
impl<T: Zeroable> Zeroable for L4<T> {}

macro_rules! impl_drop_children {
    ($parent:ty, $child:ty) => {
        impl<T> Drop for $parent {
            fn drop(&mut self) {
                for child in &self.children {
                    let ptr: *mut $child = child.load(Ordering::Acquire);
                    if !ptr.is_null() {
                        unsafe { drop(Box::from_raw(ptr)) }
                    }
                }
            }
        }
    };
}

impl_drop_children!(L1<T>, L2<T>);
impl_drop_children!(L2<T>, L3<T>);
impl_drop_children!(L3<T>, L4<T>);
// not needed for L4

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
