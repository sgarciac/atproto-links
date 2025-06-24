// @generated automatically by Diesel CLI.

diesel::table! {
    bigint_keyvals (name) {
        #[max_length = 255]
        name -> Varchar,
        bivalue -> Int8,
        created_at -> Nullable<Timestamptz>,
        updated_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    dids (did) {
        did -> Text,
        id -> Int8,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    bigint_keyvals,
    dids,
);
