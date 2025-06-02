#![allow(unused)]
#![allow(clippy::all)]

pub const JETSTREAM_CURSOR_KEY: &str = "JETSTREAM_CURSOR";

use chrono::offset::Utc;
use chrono::DateTime;
use diesel::{Insertable, Queryable, Selectable};

use crate::schema::bigint_keyvals;

#[derive(Queryable, Selectable, Insertable, Debug)]
#[diesel(primary_key(name))]
#[diesel(table_name = bigint_keyvals)]
pub struct BigintKeyval {
    pub name: String,
    pub bivalue: i64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}
