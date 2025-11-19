#!/usr/bin/env python3
# Simple console consumer for debugging (NOT Spark)

import argparse, sys
from confluent_kafka import Consumer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="realestate.mubawab.raw")
    ap.add_argument("--group", default="mubawab.dev.print")
    ap.add_argument("--offset", choices=["earliest","latest"], default="latest")
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": args.offset,
        "enable.auto.commit": True,
    })
    c.subscribe([args.topic])
    print(f"👂 Subscribed to {args.topic} ({args.bootstrap}) as {args.group}")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("ERR:", msg.error(), file=sys.stderr)
                continue
            key = msg.key().decode("utf-8") if msg.key() else "-"
            val = msg.value().decode("utf-8") if msg.value() else ""
            print(f"{key} → {val}")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    main()
