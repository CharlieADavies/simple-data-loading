create table if not exists trades (
    event_timestamp timestamp,
    instrument_id varchar(64)
);

create index on trades ( instrument_id, event_timestamp);

create table if not exists instrument_data (
  instrument_id varchar(64),
  when_timestamp timestamp,
  gamma varchar(60),
  vega varchar(60),
  theta varchar(60)
  );

create index on instrument_data (instrument_id, when_timestamp);
