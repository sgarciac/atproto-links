#![no_main]

use jetstream::exports::Did;
use bincode::config::{Configuration, BigEndian, Fixint, Limit, standard};
use bincode::serde::decode_from_slice;
use cardinality_estimator::CardinalityEstimator;
use libfuzzer_sys::fuzz_target;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

type C = Configuration<BigEndian, Fixint, Limit<1048576>>;
static BINCODE_CONF: C =
    standard()
        .with_big_endian()
        .with_fixed_int_encoding()
        .with_limit::<1048576>();

fuzz_target!(|data: &[u8]| {
    if let Ok((estimator, _n)) = decode_from_slice::<CardinalityEstimator<Did>, C>(
        data,
        BINCODE_CONF,
    ) {
        estimator.estimate();
    }
});
