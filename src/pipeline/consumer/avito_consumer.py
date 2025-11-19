#!/usr/bin/env python3
# Simple console consumer for debugging (NOT Spark)

import argparse, sys
from confluent_kafka import Consumer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="realestate.avito.raw")
    ap.add_argument("--group", default="avito.dev.print")
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    c.subscribe([args.topic])
    print(f"👂 Subscribed to {args.topic} ({args.bootstrap})")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: 
                continue
            if msg.error():
                print("ERR:", msg.error(), file=sys.stderr); 
                continue
            print(msg.key().decode("utf-8") if msg.key() else "-", "→", msg.value().decode("utf-8"))
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    main()
