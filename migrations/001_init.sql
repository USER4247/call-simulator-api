create table api_keys (
    id             serial primary key,
    key            text not null unique,
    name           text not null,
    max_concurrent integer not null default 3 check (max_concurrent > 0),
    max_cps        integer not null default 2 check (max_cps > 0),
    enabled        boolean not null default true,
    created_at     timestamptz not null default now()
);

create table calls (
    id               uuid primary key,
    api_key_id       integer not null references api_keys(id),
    from_number      text not null,
    to_number        text not null,
    metadata         jsonb not null default '{}',
    state            text not null check (state in ('QUEUED','RINGING','ANSWERED','UNANSWERED','COMPLETED')),
    created_at       timestamptz not null,
    updated_at       timestamptz not null,
    answered_at      timestamptz,
    ended_at         timestamptz,
    duration_seconds numeric(8,2),
    recording_url    text
);

create index calls_by_key_created on calls (api_key_id, created_at desc);

-- keys for local testing only. change these before deploying anywhere real
insert into api_keys (key, name, max_concurrent, max_cps) values
    ('dev-key-1', 'local dev', 3, 2),
    ('dev-key-2', 'load testing', 20, 10);
