#![no_main]

// use jetstream::exports::Did;
use ufos::db_types::DbBytes;
use ufos::store_types::EstimatedDidsValue;
use libfuzzer_sys::fuzz_target;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fuzz_target!(|data: &[u8]| {
    if let Ok((counts_value, n)) = EstimatedDidsValue::from_db_bytes(data) {
        assert!(n <= data.len());
        let serialized = counts_value.to_db_bytes().unwrap();
        assert_eq!(serialized.len(), n);
        let (and_back, n_again) = EstimatedDidsValue::from_db_bytes(&serialized).unwrap();
        assert_eq!(n_again, n);
        assert_eq!(and_back.0.estimate(), counts_value.0.estimate());
    }
});
