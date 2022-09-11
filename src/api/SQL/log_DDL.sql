create table if not exists logs (
    id serial primary key,
    url varchar(255),
    ctn varchar(10),
    result varchar(64),
    log_timestamp timestamp
);