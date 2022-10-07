import json
import logging
from dataclasses import dataclass
from datetime import datetime

from psycopg2.extras import execute_values

from get_connection import get_connection

BATCH_SIZE = 1000

conn = get_connection()
cur = conn.cursor()


def load_trade_data(messages: list[dict[str, str]]):
    sql = "insert into trades(event_timestamp, instrument_id) values %s"
    args_list = [
        (
            datetime.strptime(trade["event_timestamp"], "%Y-%m-%d %H:%M:%S.%f %Z"),
            trade["instrument_id"],
        )
        for trade in messages
    ]
    execute_values(cur, sql, args_list)
    conn.commit()
    print(f"writing {len(messages)} values to the `trades` table")


def load_instrument_data(messages: list[dict[str, str]]):
    sql = "insert into instrument_data(when_timestamp, instrument_id, gamma, vega, theta) values %s"
    args_list = [
        (
            datetime.strptime(instr["when_timestamp"], "%Y-%m-%d %H:%M:%S %Z"),
            instr["instrument_id"],
            instr["gamma"],
            instr["vega"],
            instr["theta"],
        )
        for instr in messages
    ]

    execute_values(cur, sql, args_list)
    conn.commit()
    print(f"writing {len(messages)} values to the `instrument_data` table")


with open("json_events/trades.json") as f:
    lines = [json.loads(s) for s in f.readlines()]

for batch_start in range(0, len(lines), BATCH_SIZE):
    load_trade_data(lines[batch_start : batch_start + BATCH_SIZE])

with open("json_events/valuedata.json") as f:
    lines = [json.loads(s) for s in f.readlines()]

for batch_start in range(0, len(lines), BATCH_SIZE):
    load_instrument_data(lines[batch_start : batch_start + BATCH_SIZE])


conn.close()
