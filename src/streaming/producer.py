"""
Optional: replay IEEE-CIS transactions into a Kafka topic in chronological
order, to simulate a genuinely live stream.

This is optional. If you don't want to stand up Kafka, skip this file:
Auto Loader / DLT can read directly from the bronze Delta table as a
streaming source (Delta tables support incremental streaming reads
natively), which is enough to demonstrate the pipeline mechanics without
external infra. Use this producer only if you want the "live dashboard"
demo effect for your video/GIF.

Requires: pip install kafka-python pandas
Requires a running Kafka broker (Confluent Cloud free tier works fine).
"""

import json
import time
import argparse
import pandas as pd
from kafka import KafkaProducer


def main(csv_path: str, bootstrap_servers: str, topic: str, speed: float):
    print(f"Loading {csv_path} ...")
    df = pd.read_csv(csv_path)
    df = df.sort_values("TransactionDT").reset_index(drop=True)
    print(f"Loaded {len(df)} rows, replaying in order")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    prev_dt = None
    for i, row in df.iterrows():
        record = row.where(pd.notnull(row), None).to_dict()

        # Space out sends roughly proportional to real inter-arrival time,
        # scaled by `speed` so a demo doesn't take literal days to run.
        if prev_dt is not None:
            gap_seconds = max(0, record["TransactionDT"] - prev_dt)
            time.sleep(min(gap_seconds / speed, 2.0))  # cap wait per row
        prev_dt = record["TransactionDT"]

        producer.send(topic, value=record)

        if i % 500 == 0:
            print(f"  sent {i} / {len(df)} rows")

    producer.flush()
    print("Done replaying.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-path", default="train_transaction.csv")
    parser.add_argument("--bootstrap-servers", required=True,
                         help="e.g. localhost:9092 or your Confluent Cloud broker")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--speed", type=float, default=200.0,
                         help="Replay speed multiplier (higher = faster). 200x turns "
                              "roughly a day of data into a few minutes.")
    args = parser.parse_args()
    main(args.csv_path, args.bootstrap_servers, args.topic, args.speed)
