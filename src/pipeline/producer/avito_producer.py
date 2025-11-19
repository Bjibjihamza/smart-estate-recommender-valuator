import argparse
from pipeline.extract.avito_scraper import run_job


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["louer", "vendre", "acheter"], default="louer")
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="realestate.avito.raw")
    ap.add_argument("--serp-delay", type=float, default=1.5)
    ap.add_argument("--detail-delay", type=float, default=1.5)

    # NEW: start page
    ap.add_argument("--start-page", type=int, default=1)

    args = ap.parse_args()

    run_job(
        mode=args.mode,
        num_pages=args.pages,
        sink="kafka",
        kafka_bootstrap=args.bootstrap,
        kafka_topic=args.topic,
        serp_delay=args.serp_delay,
        detail_delay=args.detail_delay,
        max_items=args.limit,
        start_page=args.start_page,  # NEW
    )


if __name__ == "__main__":
    main()
