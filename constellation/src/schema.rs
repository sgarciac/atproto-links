// @generated automatically by Diesel CLI.

diesel::table! {
    jetstream_cursor (id) {
        id -> Int4,
        savepoint -> Text,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}
