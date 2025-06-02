-- a migration for creating a table where to store the jetstream cursor, 
create table if not exists bigint_keyvals (
    name varchar(255) not null primary key,
    bivalue bigint not null,
    created_at timestamp with time zone default now(),
    updated_at timestamp with time zone default now()
);
