//!
//! # Disk Storage Implementation
//!

use crate::{
    basic::mapx_raw::{MapxRaw, MapxRawIter},
    common::InstanceCfg,
    OrderConsistKey,
};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use sled::IVec;
use std::{
    fmt,
    iter::{DoubleEndedIterator, Iterator},
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

// To solve the problem of unlimited memory usage,
// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub(super) struct MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    inner: MapxRaw,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> From<InstanceCfg> for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: MapxRaw::from(cfg),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }
}

impl<K, V> From<&MapxOC<K, V>> for InstanceCfg
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(x: &MapxOC<K, V>) -> Self {
        let cfg = x.inner.get_instance_cfg();
        Self {
            prefix: cfg.prefix,
            item_cnt: cfg.item_cnt,
            data_set_idx: cfg.data_set_idx,
        }
    }
}

///////////////////////////////////////////////////////
// Begin of the self-implementation of backend::MapxOC //
/*****************************************************/

impl<K, V> MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    // create a new instance
    #[inline(always)]
    pub(super) fn must_new() -> Self {
        MapxOC {
            inner: MapxRaw::new(),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    // Get the storage path
    pub(super) fn get_instance_cfg(&self) -> InstanceCfg {
        InstanceCfg::from(self)
    }

    // Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub(super) fn get(&self, key: &K) -> Option<V> {
        self.inner
            .get(&key.to_bytes())
            .map(|bytes| pnk!(bcs::from_bytes(&bytes)))
    }

    #[inline(always)]
    pub(super) fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_le(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(bcs::from_bytes(&v))))
    }

    #[inline(always)]
    pub(super) fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_ge(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(bcs::from_bytes(&v))))
    }

    // Imitate the behavior of 'BTreeMap<_>.len()'.
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    // Imitate the behavior of 'BTreeMap<_>.insert(...)'.
    #[inline(always)]
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.set_value(key, value)
            .map(|v| pnk!(bcs::from_bytes(&v)))
    }

    // Similar with `insert`, but ignore if the old value is exist.
    #[inline(always)]
    pub(super) fn set_value(&mut self, key: K, value: V) -> Option<IVec> {
        self.inner
            .insert(&key.into_bytes(), &pnk!(bcs::to_bytes(&value)))
    }

    // Imitate the behavior of '.iter()'
    #[inline(always)]
    pub(super) fn iter(&self) -> MapxOCIter<K, V> {
        MapxOCIter {
            iter: self.inner.iter(),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOCIter<K, V> {
        let ll;
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                ll = lo.to_bytes();
                Bound::Included(ll.as_slice())
            }
            Bound::Excluded(lo) => {
                ll = lo.to_bytes();
                Bound::Excluded(ll.as_slice())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let hh;
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                hh = hi.to_bytes();
                Bound::Included(hh.as_slice())
            }
            Bound::Excluded(hi) => {
                hh = hi.to_bytes();
                Bound::Excluded(hh.as_slice())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOCIter {
            iter: self.inner.range((l, h)),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    #[inline(always)]
    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.to_bytes())
    }

    #[inline(always)]
    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        self.unset_value(key).map(|v| pnk!(bcs::from_bytes(&v)))
    }

    #[inline(always)]
    pub(super) fn unset_value(&mut self, key: &K) -> Option<IVec> {
        self.inner.remove(&key.to_bytes())
    }

    #[inline(always)]
    pub(super) fn clear(&mut self) {
        self.inner.clear();
    }
}

/***************************************************/
// End of the self-implementation of backend::MapxOC //
/////////////////////////////////////////////////////

///////////////////////////////////////////////////////////
// Begin of the implementation of Iter for backend::MapxOC //
/*********************************************************/

// Iter over [MapxOC](self::Mapxnk).
pub(super) struct MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    pub(super) iter: MapxRawIter,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> Iterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(bcs::from_bytes(&v))))
    }
}

impl<K, V> DoubleEndedIterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (pnk!(K::from_slice(&k)), pnk!(bcs::from_bytes(&v))))
    }
}

impl<K, V> ExactSizeIterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
}

/*******************************************************/
// End of the implementation of Iter for backend::MapxOC //
/////////////////////////////////////////////////////////