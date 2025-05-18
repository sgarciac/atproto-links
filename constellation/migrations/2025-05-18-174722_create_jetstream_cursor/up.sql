-- a migration for creating a table where to store the jetstream latest read cursor
create table if not exists jetstream_cursor (
    id integer primary key,
    savepoint text not null,
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone not null default now()
);
