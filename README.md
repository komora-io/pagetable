# pagetable

Wait-free 4-level page table that maps from a `u64` key to an `&AtomicU64` value. Page fan-out is 2^16.
If a key doesn't exist, intermediate pages are atomically created while traversing down the levels,
and the value is initialized to `0`.

This is a somewhat specialized data structure, but it is useful for maintaining metadata in
concurrent systems that need to track many items that have an associated logical ID that is
allocated from a dense keyspace, like databases that would like to keep track of where a page
lives based on its 64-bit ID, despite it being rewritten in random places during defragmentation.

# API

```rust
#[derive(Default)]
pub struct PageTable { .. }

pub fn get(&self, key: u64) -> &AtomicU64 { .. }
```

# Example

```rust
let pt = PageTable::default();

for i in 0..100_000_000 {
    pt.get(i).fetch_add(1, Ordering::SeqCst);
}

for i in 0..100_000_000 {
    let value = pt.get(i).load(Ordering::SeqCst);
    assert_eq!(value, 1);
}
```
