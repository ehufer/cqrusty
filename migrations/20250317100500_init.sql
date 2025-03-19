create table if not exists events 
(
    id int primary key,
    stream_id int,
    payload text,
    time_stamp int 
);