create view trade_data_with_offsets as (
  with five_s as (
    select t.instrument_id as instr, t.event_timestamp as seed_time, i.when_timestamp as offset_time, i.gamma, i.vega, i.theta, row_number() over (partition by (i.instrument_id, t.event_timestamp) order by i.when_timestamp DESC) as rn
    from instrument_data as i
    join trades as t on i.instrument_id = t.instrument_id
    where i.when_timestamp < t.event_timestamp + interval '5 second' and i.when_timestamp > t.event_timestamp
  ), one_m as (
    select t.instrument_id as instr, t.event_timestamp as seed_time, i.when_timestamp as offset_time, i.gamma, i.vega, i.theta, row_number() over (partition by (i.instrument_id, t.event_timestamp) order by i.when_timestamp DESC) as rn
    from instrument_data as i
    join trades as t on i.instrument_id = t.instrument_id
    where i.when_timestamp < t.event_timestamp + interval '1 minute' and i.when_timestamp > t.event_timestamp
  ), thirty_m as (
    select t.instrument_id as instr, t.event_timestamp as seed_time, i.when_timestamp as offset_time, i.gamma, i.vega, i.theta, row_number() over (partition by (i.instrument_id, t.event_timestamp) order by i.when_timestamp DESC) as rn
    from instrument_data as i
    join trades as t on i.instrument_id = t.instrument_id
    where i.when_timestamp < t.event_timestamp + interval '30 minute' and i.when_timestamp > t.event_timestamp
  ), sixty_m as (
    select t.instrument_id as instr, t.event_timestamp as seed_time, i.when_timestamp as offset_time, i.gamma, i.vega, i.theta, row_number() over (partition by (i.instrument_id, t.event_timestamp) order by i.when_timestamp DESC) as rn
    from instrument_data as i
    join trades as t on i.instrument_id = t.instrument_id
    where i.when_timestamp < t.event_timestamp + interval '1 hour' and i.when_timestamp > t.event_timestamp
  )

select five_s.instr as instrument_id,
    five_s.seed_time as when_timestamp,

    five_s.gamma as gamma_5s,
    one_m.gamma as gamma_1m,
    thirty_m.gamma as gamma_30m,
    sixty_m.gamma as gamma_60m,

    five_s.vega as vega_5s,
    one_m.vega as vega_1m,
    thirty_m.vega as vega_30m,
    sixty_m.vega as vega_60m,

    five_s.theta as theta_5s,
    one_m.theta as theta_1m,
    thirty_m.theta as theta_30m,
    sixty_m.theta as theta_60m 

  from five_s as five_s
    join one_m on five_s.instr = one_m.instr and five_s.seed_time = one_m.seed_time and five_s.rn = 1 and one_m.rn = 1
    join thirty_m on five_s.instr = thirty_m.instr and five_s.seed_time = thirty_m.seed_time and five_s.rn = 1 and thirty_m.rn = 1
    join sixty_m on five_s.instr = sixty_m.instr and five_s.seed_time = sixty_m.seed_time and five_s.rn = 1 and sixty_m.rn = 1
);
