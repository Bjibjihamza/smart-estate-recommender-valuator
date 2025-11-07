#!/usr/bin/env python3
# Run:
#   python -m pipeline.producer.mubawab_producer --mode louer --pages 1 \
#     --bootstrap kafka:9092 --topic realestate.mubawab.raw --limit 5

import argparse
from pipeline.extract.mubawab_scraper import run_job

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["louer", "vendre", "acheter"], default="louer")
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--per-page", type=int, default=10)
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="realestate.mubawab.raw")
    ap.add_argument("--serp-delay", type=float, default=1.3)
    ap.add_argument("--detail-delay", type=float, default=1.3)
    args = ap.parse_args()

    run_job(
        mode=args.mode,
        num_pages=args.pages,
        sink="kafka",
        kafka_bootstrap=args.bootstrap,
        kafka_topic=args.topic,
        serp_delay=args.serp_delay,
        detail_delay=args.detail_delay,
        per_page=args.per_page,
        max_items=args.limit,
    )

if __name__ == "__main__":
    main()
