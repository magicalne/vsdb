use crate::common::{
    vsdb_get_base_dir, vsdb_set_base_dir, Engine, Pre, PreBytes, RawKey, RawValue,
    PREFIX_SIZE, RESERVED_ID_CNT,
};
use parking_lot::Mutex;
use ruc::*;
use sled::Db;
use std::{
    borrow::Cow,
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicUsize, Ordering},
        LazyLock,
    },
};

// NOTE:
// do NOT make the number of areas bigger than `u8::MAX`
const DATA_SET_NUM: usize = 2;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

// NOTE: `Sled` doesn't have column family. So there is no way to dinstinguish between `instance_prefix` and a normal `key`.
// Add a `prefix` here to make sure the Instance Prefix is unique.
const META_INSTANCE_PREFIX: [u8; 1] = [1];

static HDR: LazyLock<Db> = LazyLock::new(|| sled_open().unwrap());

pub struct SledEngine {
    meta: &'static Db,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

impl SledEngine {
    #[inline(always)]
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        self.max_keylen.store(len, Ordering::Relaxed);
        self.meta
            .insert(META_KEY_MAX_KEYLEN, &len.to_be_bytes())
            .unwrap();
    }

    #[inline(always)]
    fn get_upper_bound_value(&self, meta_prefix: PreBytes) -> Vec<u8> {
        const BUF: [u8; 256] = [u8::MAX; 256];

        let mut max_guard = meta_prefix.to_vec();

        let l = self.get_max_keylen();
        if l < 257 {
            max_guard.extend_from_slice(&BUF[..l]);
        } else {
            max_guard.extend_from_slice(&vec![u8::MAX; l]);
        }

        max_guard
    }
}

impl Engine for SledEngine {
    fn new() -> Result<Self> {
        let meta = &HDR;

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if meta.get(META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            meta.insert(META_KEY_MAX_KEYLEN, &0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(prefix_allocator.key).c(d!())?.is_none() {
            meta.insert(prefix_allocator.key, &initial_value).c(d!())?;
        }

        let max_keylen = AtomicUsize::new(crate::parse_int!(
            meta.get(META_KEY_MAX_KEYLEN).unwrap().unwrap(),
            usize
        ));

        Ok(SledEngine {
            meta,
            prefix_allocator,
            // length of the raw key, exclude the meta prefix
            max_keylen,
        })
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we use a `Mutex` lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        static LK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
        let x = LK.lock();

        // step 1
        let ret = crate::parse_prefix!(self
            .meta
            .get(self.prefix_allocator.key)
            .unwrap()
            .unwrap());

        // step 2
        self.meta
            .insert(self.prefix_allocator.key, &(1 + ret).to_be_bytes())
            .unwrap();

        ret
    }

    fn area_count(&self) -> usize {
        DATA_SET_NUM
    }

    fn flush(&self) {
        self.meta.flush().unwrap();
    }

    fn iter(&self, meta_prefix: PreBytes) -> SledIter {
        let inner = self.meta.scan_prefix(meta_prefix);

        SledIter { inner }
    }

    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> SledIter {
        let mut b_lo = meta_prefix.to_vec();
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                b_lo.as_slice()
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                b_lo.push(0u8);
                b_lo.as_slice()
            }
            _ => meta_prefix.as_slice(),
        };

        let mut b_hi = meta_prefix.to_vec();
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                b_hi.push(0u8);
                b_hi
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                b_hi
            }
            _ => self.get_upper_bound_value(meta_prefix),
        };
        let h = h.as_slice();

        let inner = self.meta.range(l..h);

        SledIter { inner }
    }

    fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        self.meta.get(k).unwrap().map(|v| v.to_vec())
    }

    fn insert(
        &self,
        meta_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        self.meta.insert(k, value).unwrap().map(|v| v.to_vec())
    }

    fn remove(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        let old_v = self.meta.get(k.clone()).unwrap().map(|v| v.to_vec());
        self.meta.remove(k).unwrap();
        old_v
    }

    fn get_instance_len_hint(&self, prefix: PreBytes) -> u64 {
        let instance_prefix: Vec<u8> = get_instance_prefix(&prefix);
        crate::parse_int!(self.meta.get(&instance_prefix).unwrap().unwrap(), u64)
    }

    fn set_instance_len_hint(&self, prefix: PreBytes, new_len: u64) {
        let instance_prefix: Vec<u8> = get_instance_prefix(&prefix);
        self.meta
            .insert(instance_prefix, &new_len.to_be_bytes())
            .unwrap();
    }
}

pub struct SledIter {
    inner: sled::Iter,
}

impl Iterator for SledIter {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|v| v.unwrap())
            .map(|(ik, iv)| (ik[PREFIX_SIZE..].to_vec(), iv.to_vec()))
    }
}

impl DoubleEndedIterator for SledIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|v| v.unwrap())
            .map(|(ik, iv)| (ik[PREFIX_SIZE..].to_vec(), iv.to_vec()))
    }
}

// key of the prefix allocator in the 'meta'
struct PreAllocator {
    key: [u8; 1],
}

impl PreAllocator {
    const fn init() -> (Self, PreBytes) {
        (
            Self {
                key: META_KEY_PREFIX_ALLOCATOR,
            },
            (RESERVED_ID_CNT + Pre::MIN).to_be_bytes(),
        )
    }
}

fn sled_open() -> Result<Db> {
    let dir = vsdb_get_base_dir();

    // avoid setting again on an opened DB
    omit!(vsdb_set_base_dir(&dir));

    let db = sled::open(&dir).c(d!())?;

    Ok(db)
}

fn get_instance_prefix(prefix: &[u8]) -> Vec<u8> {
    META_INSTANCE_PREFIX
        .iter()
        .chain(prefix.iter())
        .cloned()
        .collect()
}
