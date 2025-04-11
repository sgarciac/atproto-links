#![no_main]

// use jetstream::exports::Did;
use ufos::db_types::DbBytes;
use ufos::store_types::CountsValue;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok((counts_value, n)) = CountsValue::from_db_bytes(data) {
        assert!(n <= data.len());
        let serialized = counts_value.to_db_bytes().unwrap();
        assert_eq!(serialized.len(), n);
        let (and_back, n_again) = CountsValue::from_db_bytes(&serialized).unwrap();
        assert_eq!(n_again, n);
        assert_eq!(and_back.records(), counts_value.records());
        assert_eq!(and_back.dids().estimate(), counts_value.dids().estimate());
        // assert_eq!(serialized, data[..n]);
        // counts_value.prefix.0 += 1;
        // counts_value.suffix.0.insert(&Did::new("did:plc:blah".to_string()).unwrap());
        // assert!(counts_value.records() > 0);
        // assert!(counts_value.dids().estimate() > 0);
    }
});
