//!
//! # Test Cases
//!

use super::*;
use serde::{Deserialize, Serialize};
use std::ops::Bound;

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone)]
struct SampleBlock {
    idx: usize,
    data: Vec<usize>,
}

fn gen_sample(idx: usize) -> SampleBlock {
    SampleBlock {
        idx,
        data: vec![idx],
    }
}

#[test]
fn t_mapx_oc() {
    crate::clear();

    let cnt = 200;

    let db = {
        let mut dbi = crate::MapxOC::new();

        assert_eq!(0, dbi.len());
        (0..cnt).for_each(|i| {
            assert!(dbi.get(&i).is_none());
        });

        (0..cnt).map(|i| (i, gen_sample(i))).for_each(|(i, b)| {
            dbi.entry(i).or_insert(b.clone());
            assert_eq!(1 + i as usize, dbi.len());
            assert_eq!(pnk!(dbi.get(&i)).idx, i);
            assert_eq!(dbi.remove(&i), Some(b.clone()));
            assert_eq!(i as usize, dbi.len());
            assert!(dbi.get(&i).is_none());
            assert!(dbi.insert(i, b.clone()).is_none());
            assert!(dbi.insert(i, b).is_some());
        });

        assert_eq!(cnt, dbi.len());

        pnk!(bincode::serialize(&dbi))
    };

    let mut db_restore = pnk!(bincode::deserialize::<MapxOC<usize, SampleBlock>>(&db));

    assert_eq!(cnt, db_restore.len());

    (0..cnt).for_each(|i| {
        assert_eq!(i, db_restore.get(&i).unwrap().idx);
    });

    (1..cnt).for_each(|i| {
        pnk!(db_restore.get_mut(&i)).idx = 1 + i;
        assert_eq!(pnk!(db_restore.get(&i)).idx, 1 + i);
        assert!(db_restore.contains_key(&i));
        assert!(db_restore.remove(&i).is_some());
        assert!(!db_restore.contains_key(&i));
    });

    assert_eq!(1, db_restore.len());
    crate::clear();
    unsafe { db_restore.set_len(0) };
    assert!(db_restore.is_empty());

    db_restore.insert(1, gen_sample(1));
    db_restore.insert(10, gen_sample(10));
    db_restore.insert(100, gen_sample(100));
    db_restore.insert(1000, gen_sample(1000));

    assert!(db_restore.range(0..1).next().is_none());

    assert_eq!(100, db_restore.range(12..999).next().unwrap().1.idx);
    assert_eq!(100, db_restore.range(12..=999).next().unwrap().1.idx);

    assert_eq!(100, db_restore.range(100..=999).next().unwrap().1.idx);
    assert!(db_restore
        .range((Bound::Excluded(100), Bound::Included(999)))
        .next()
        .is_none());

    assert_eq!(100, db_restore.get_ge(&99).unwrap().1.idx);
    assert_eq!(100, db_restore.get_ge(&100).unwrap().1.idx);
    assert_eq!(100, db_restore.get_le(&100).unwrap().1.idx);
    assert_eq!(100, db_restore.get_le(&101).unwrap().1.idx);
}
