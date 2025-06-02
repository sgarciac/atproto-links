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
