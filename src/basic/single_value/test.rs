use super::SingleValue;
use serde::{Deserialize, Serialize};

#[test]
fn basic_cases() {
    assert_eq!(SingleValue::new(0), 0);
    assert!(SingleValue::new(1) > 0);
    assert!(SingleValue::new(1) >= 0);
    assert!(SingleValue::new(0) < 1);
    assert!(SingleValue::new(0) <= 1);

    assert_eq!(SingleValue::new(0), SingleValue::new(0));
    assert!(SingleValue::new(1) > SingleValue::new(0));
    assert!(SingleValue::new(1) >= SingleValue::new(1));
    assert!(SingleValue::new(0) < SingleValue::new(1));
    assert!(SingleValue::new(1) <= SingleValue::new(1));

    assert_eq!(SingleValue::new(1) + 1, 2);
    assert_eq!(SingleValue::new(1) - 1, 0);
    assert_eq!(SingleValue::new(1) * 1, 1);
    assert_eq!(SingleValue::new(1) / 2, 0);
    assert_eq!(SingleValue::new(1) % 2, 1);

    assert_eq!(-SingleValue::new(1), -1);
    assert_eq!(!SingleValue::new(1), !1);

    assert_eq!(SingleValue::new(1) >> 2, 1 >> 2);
    assert_eq!(SingleValue::new(1) << 2, 1 << 2);

    assert_eq!(SingleValue::new(1) | 2, 1 | 2);
    assert_eq!(SingleValue::new(1) & 2, 1 & 2);
    assert_eq!(SingleValue::new(1) ^ 2, 1 ^ 2);

    let mut v = SingleValue::new(1);
    v += 1;
    assert_eq!(v, 2);
    v *= 100;
    assert_eq!(v, 200);
    v -= 1;
    assert_eq!(v, 199);
    v /= 10;
    assert_eq!(v, 19);
    v %= 10;
    assert_eq!(v, 9);

    *v.get_mut() = -v.clone_inner();
    assert_eq!(v, -9);

    *v.get_mut() = !v.clone_inner();
    assert_eq!(v, !-9);

    *v.get_mut() = 0;
    v >>= 2;
    assert_eq!(v, 0 >> 2);

    *v.get_mut() = 0;
    v <<= 2;
    assert_eq!(v, 0 << 2);

    *v.get_mut() = 0;
    v |= 2;
    assert_eq!(v, 0 | 2);

    *v.get_mut() = 0;
    v &= 2;
    assert_eq!(v, 0 & 2);

    *v.get_mut() = 0;
    v ^= 2;
    assert_eq!(v, 0 ^ 2);
}

#[test]
fn custom_types() {
    #[derive(
        Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize,
    )]
    struct Foo {
        a: i32,
        b: String,
        c: bool,
    }

    assert_eq!(SingleValue::new(Foo::default()), Foo::default());
    assert_eq!(
        SingleValue::new(Foo::default()),
        SingleValue::new(Foo::default())
    );

    assert!(
        SingleValue::new(Foo::default())
            < Foo {
                a: 1,
                b: "".to_string(),
                c: true
            }
    );
    assert!(
        SingleValue::new(Foo::default())
            <= Foo {
                a: 1,
                b: "".to_string(),
                c: true
            }
    );

    assert!(SingleValue::new(Foo::default()) >= Foo::default());
    assert!(SingleValue::new(Foo::default()) >= SingleValue::new(Foo::default()));
}