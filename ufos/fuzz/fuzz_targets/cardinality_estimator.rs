#![no_main]

use bincode::config::{Configuration, LittleEndian, Varint, Limit, standard};
use bincode::serde::decode_from_slice;
use cardinality_estimator::CardinalityEstimator;
use libfuzzer_sys::fuzz_target;

type C = Configuration<LittleEndian, Varint, Limit<1048576>>;
static BINCODE_CONF: C = standard().with_limit::<1048576>();

fuzz_target!(|data: &[u8]| {
    if let Ok((mut estimator, _n)) = decode_from_slice::<CardinalityEstimator<String>, C>(
        data,
        BINCODE_CONF,
    ) {
        // crash happens *much* faster if we just do kinda anything with the estimator
        estimator.insert(&"asdf".to_string());
        assert!(estimator.estimate() > 0);
    }
});
