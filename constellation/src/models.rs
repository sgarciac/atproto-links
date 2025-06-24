#![allow(unused)]
#![allow(clippy::all)]

pub const JETSTREAM_CURSOR_KEY: &str = "JETSTREAM_CURSOR";

use chrono::offset::Utc;
use chrono::DateTime;
use diesel::{prelude::Identifiable, Insertable, Queryable, Selectable};

use crate::schema::bigint_keyvals;
use crate::schema::dids;

#[derive(Queryable, Selectable, Insertable, Debug)]
#[diesel(primary_key(name))]
#[diesel(table_name = bigint_keyvals)]
pub struct BigintKeyval {
    pub name: String,
    pub bivalue: i64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Queryable, Debug, Selectable, Insertable, Identifiable)]
#[diesel(table_name = dids)]
#[diesel(primary_key(id))]
pub struct Did {
    #[diesel(deserialize_as = i64)]
    pub id: Option<i64>,
    pub did: String,
}
