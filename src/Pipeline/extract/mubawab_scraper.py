#!/usr/bin/env python3
# Scrape Mubawab listings → rows (dicts) and optionally push to Kafka (via run_job)
# Contract mirrors avito_scraper.run_job so your producer pattern stays identical.

import argparse
import csv
import json
import random
import re
import time
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_DEFAULT = "kafka:9092"
KAFKA_TOPIC_DEFAULT = "realestate.mubawab.raw"

BASE = "https://www.mubawab.ma"
LISTING_LOUER = "https://www.mubawab.ma/fr/sc/appartements-a-louer:o:n"
LISTING_VENDRE = "https://www.mubawab.ma/fr/sc/appartements-a-vendre:o:n"

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
]

ID_RE = re.compile(r"/a/(\d+)/")
PRICE_NUM_RE = re.compile(r"[\d\s]+")


def pick_headers(i: int) -> Dict[str, str]:
    return {
        "User-Agent": UA_LIST[i % len(UA_LIST)],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def clean_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    t = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def page_url(mode: str, page: int) -> str:
    base = LISTING_VENDRE if mode == "vendre" else LISTING_LOUER
    if page <= 1:
        return base
    # Mubawab paginates with :p:N
    return f"{base}:p:{page}"


def extract_firstN_links(html: str, limit: int = 25) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    seen: Set[str] = set()

    # Card boxes with direct linkref
    for box in soup.select('div.listingBox[linkref]'):
        href = box.get("linkref", "").strip()
        if href and href.startswith("http") and "/fr/a/" in href:
            if href not in seen:
                seen.add(href)
                links.append(href)
                if len(links) >= limit:
                    return links

    # Titles
    for a in soup.select("h2.listingTit a[href]"):
        href = a.get("href", "").strip()
        if href.startswith("/"):
            href = urljoin(BASE, href)
        if href.startswith("http") and "/fr/a/" in href:
            if href not in seen:
                seen.add(href)
                links.append(href)
                if len(links) >= limit:
                    return links

    # Fallback: any ad link
    for a in soup.select('a[href*="/fr/a/"]'):
        href = a.get("href", "").strip()
        if href.startswith("/"):
            href = urljoin(BASE, href)
        if href.startswith("http") and href not in seen:
            seen.add(href)
            links.append(href)
            if len(links) >= limit:
                return links

    return links[:limit]


def get_id_from_url(url: str) -> Optional[str]:
    m = ID_RE.search(url)
    return m.group(1) if m else None


def parse_price_number(price_text: Optional[str]) -> Optional[int]:
    if not price_text:
        return None
    m = PRICE_NUM_RE.search(price_text.replace("\u00a0", " "))
    if not m:
        return None
    try:
        return int(m.group(0).replace(" ", ""))
    except Exception:
        return None


def parse_listing_details(html: str) -> Dict[str, Any]:
    """
    Extract key fields from a Mubawab ad page (robust, DOM-safe).
    """
    soup = BeautifulSoup(html, "lxml")
    out: Dict[str, Any] = {}

    # Title
    title_el = soup.select_one("h1.searchTitle")
    out["title"] = clean_text(title_el.get_text()) if title_el else None

    # Price (formatted + numeric)
    price_el = soup.select_one("h3.orangeTit")
    price_text = clean_text(price_el.get_text()) if price_el else None
    out["price_text"] = price_text
    out["price_number"] = parse_price_number(price_text)

    # Location (often "Quartier à Ville")
    loc_el = soup.select_one("h3.greyTit")
    out["location_text"] = clean_text(loc_el.get_text()) if loc_el else None

    # Small features block (e.g. pièces/chambres/sdb)
    ad_details = []
    for feat in soup.select("div.adDetails div.adDetailFeature"):
        txt = clean_text(feat.get_text())
        if txt:
            ad_details.append(txt)
    out["ad_details_text"] = json.dumps(ad_details, ensure_ascii=False) if ad_details else None

    # Amenities (icons like Terrasse, Ascenseur…)
    amenities = []
    for feat in soup.select("div.adFeatures div.adFeature span"):
        txt = clean_text(feat.get_text())
        if txt:
            amenities.append(txt)
    out["features_amenities_json"] = json.dumps(amenities, ensure_ascii=False) if amenities else None

    # Description (first paragraph in blockProp)
    desc_block = soup.select_one("div.blockProp")
    desc = None
    if desc_block:
        p_tag = desc_block.find("p")
        if p_tag:
            desc = clean_text(p_tag.get_text())
    out["description_text"] = desc

    # Main characteristics (label/value pairs)
    attr_dict: Dict[str, str] = {}
    for feature in soup.select("div.caractBlockProp div.adMainFeature"):
        label_el = feature.select_one("p.adMainFeatureContentLabel")
        value_el = feature.select_one("p.adMainFeatureContentValue")
        if label_el and value_el:
            k = clean_text(label_el.get_text())
            v = clean_text(value_el.get_text())
            if k and v:
                attr_dict[k] = v
    out["features_main_json"] = json.dumps(attr_dict, ensure_ascii=False) if attr_dict else None

    # Images (gallery + slider)
    imgs: List[str] = []
    for img in soup.select("div.picturesGallery img[src]"):
        src = img.get("src")
        if src and "mubawab-media.com/ad/" in src:
            imgs.append(src)
    for img in soup.select("div.flipsnap img[src]"):
        src = img.get("src")
        if src and "mubawab-media.com/ad/" in src:
            imgs.append(src)
    dedup, seen = [], set()
    for u in imgs:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    out["gallery_urls"] = json.dumps(dedup, ensure_ascii=False) if dedup else None

    # Agency (optional)
    agency_name = agency_url = agency_logo = None
    business_info = soup.select_one("div.businessInfo")
    if business_info:
        name_el = business_info.select_one("span.businessName")
        if name_el:
            agency_name = clean_text(name_el.get_text())
        logo_el = business_info.select_one("img[src]")
        if logo_el:
            agency_logo = logo_el.get("src")
            if agency_logo and agency_logo.startswith("/"):
                agency_logo = urljoin(BASE, agency_logo)
        link_el = business_info.select_one("a[href]")
        if link_el:
            href = link_el.get("href", "").strip()
            if href.startswith("/"):
                href = urljoin(BASE, href)
            agency_url = href if href.startswith("http") else None
    out["agency_name"] = agency_name
    out["agency_url"] = agency_url
    out["agency_logo"] = agency_logo

    # Flags (presence)
    out["has_phone_box"] = True if soup.select_one(".phone-number-box, .contactPhoneClick") else False
    out["has_map_block"] = True if soup.select_one(".prop-map-holder, #mapOpen, #mapClosed") else False

    return out


def build_producer(bootstrap: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.num.messages": 10000,
    }
    return Producer(conf)


def delivery_cb(err, msg):
    if err:
        print(f"[DLVRY_ERROR] {err} (topic={msg.topic()} key={msg.key()})")


def send_to_kafka(row: Dict[str, Any], producer: Producer, kafka_topic: str):
    key = (row.get("id") or "").encode("utf-8")
    value = json.dumps(row, ensure_ascii=False).encode("utf-8")
    producer.produce(kafka_topic, key=key, value=value, on_delivery=delivery_cb)
    producer.poll(0)


def fetch_details(session: requests.Session, url: str, i: int, delay: float) -> Dict[str, Any]:
    data: Dict[str, Any] = {"id": get_id_from_url(url), "url": url, "error": None}
    try:
        r = session.get(url, headers=pick_headers(i), timeout=30)
        r.raise_for_status()
        details = parse_listing_details(r.text)
        data.update(details)
    except Exception as e:
        data["error"] = str(e)
    finally:
        time.sleep(delay + random.random() * 0.5)
    return data


def crawl_serp(mode: str, pages: int, per_page: Optional[int], serp_delay: float) -> List[str]:
    session = requests.Session()
    all_links: List[str] = []
    url = None
    for p in range(1, pages + 1):
        url = page_url(mode, p)
        print(f"[SERP] Page {p} → {url}")
        resp = session.get(url, headers=pick_headers(p), timeout=30)
        resp.raise_for_status()
        links = extract_firstN_links(resp.text, limit=per_page or 25)
        print(f"       +{len(links)} links")
        # Keep unique order
        for u in links:
            if u not in all_links:
                all_links.append(u)
        if p < pages:
            time.sleep(serp_delay)
    return all_links


def run_job(
    mode: str,
    num_pages: int = 1,
    sink: str = "kafka",
    out_csv: str = "mubawab_out.csv",
    kafka_bootstrap: str = KAFKA_BOOTSTRAP_DEFAULT,
    kafka_topic: str = KAFKA_TOPIC_DEFAULT,
    serp_delay: float = 1.3,
    detail_delay: float = 1.3,
    per_page: Optional[int] = None,
    max_items: Optional[int] = None,
):
    # normalize mode like Avito (support 'acheter' alias)
    mode = "vendre" if mode in ("vendre", "acheter") else "louer"

    print(f"[*] MUBAWAB | Mode={mode} | Pages={num_pages} | Sink={sink} | Limit={max_items} | PerPage={per_page}")
    links = crawl_serp(mode, num_pages, per_page, serp_delay)
    if max_items is not None:
        links = links[:max_items]
    print(f"[*] Total links: {len(links)}")

    session = requests.Session()
    rows: List[Dict[str, Any]] = []
    producer = build_producer(kafka_bootstrap) if sink == "kafka" else None

    for i, u in enumerate(links, 1):
        print(f"[{i}/{len(links)}] {u}")
        row = fetch_details(session, u, i, detail_delay)
        rows.append(row)
        if producer:
            send_to_kafka(row, producer, kafka_topic)
        if max_items is not None and i >= max_items:
            break

    if producer:
        producer.flush(10)

    if sink == "csv":
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            if rows:
                fieldnames = list(rows[0].keys())
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", quoting=csv.QUOTE_ALL)
                w.writeheader()
                for r in rows:
                    # basic cleanup
                    for k, v in list(r.items()):
                        if isinstance(v, str):
                            r[k] = clean_text(v)
                    w.writerow(r)
        print(f"[✓] Wrote {len(rows)} rows → {out_csv}")
    else:
        print(f"[✓] Sent {len(rows)} messages to Kafka topic '{kafka_topic}'")


# Optional: local CLI for quick CSV runs (kept similar to Avito)
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scrape Mubawab listings (links + details).")
    ap.add_argument("--mode", choices=["louer", "vendre", "acheter"], default="louer")
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--per-page", type=int, default=25)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sink", choices=["kafka", "csv"], default="csv")
    ap.add_argument("--out", default="mubawab_out.csv")
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP_DEFAULT)
    ap.add_argument("--topic", default=KAFKA_TOPIC_DEFAULT)
    ap.add_argument("--serp-delay", type=float, default=1.3)
    ap.add_argument("--detail-delay", type=float, default=1.3)
    args = ap.parse_args()

    run_job(
        mode=args.mode,
        num_pages=args.pages,
        sink=args.sink,
        out_csv=args.out,
        kafka_bootstrap=args.bootstrap,
        kafka_topic=args.topic,
        serp_delay=args.serp_delay,
        detail_delay=args.detail_delay,
        per_page=args.per_page,
        max_items=args.limit,
    )
