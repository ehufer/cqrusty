drop table events;
create table if not exists events 
(
    stream_id   bigint,
    version     smallint,
    payload     text,
    event_id    uuid,
    created_at  timestamptz,
    primary key(stream_id, version)
);