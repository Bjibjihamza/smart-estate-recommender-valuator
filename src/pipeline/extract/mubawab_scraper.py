#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Mubawab CC Scraper (all building categories, newest first, no surface filters)

Outputs one clean record per listing (ready for Silver):
  - id, url, error
  - title
  - price (numeric MAD)                 <-- single numeric field
  - location_text
  - description_text
  - features_main_json                  <-- JSON map as string (incl. adDetails)
  - features_amenities_json             <-- JSON array as string
  - gallery_urls                        <-- JSON array as string
  - agency_name, agency_url
  - listing_type                        <-- 'vente' or 'location'
"""

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

# ----------------------------
# Defaults / Constants
# ----------------------------
KAFKA_BOOTSTRAP_DEFAULT = "kafka:9092"
KAFKA_TOPIC_DEFAULT = "realestate.mubawab.raw"

BASE = "https://www.mubawab.ma"

# CC categories (sale vs rent)
CC_DEFAULT_CATEGORIES_SALE = (
    "apartment-sale,commercial-sale,farm-sale,house-sale,land-sale,"
    "office-sale,other-sale,riad-sale,villa-sale"
)
CC_DEFAULT_CATEGORIES_RENT = (
    "apartment-rent,commercial-rent,farm-rent,house-rent,land-rent,"
    "office-rent,other-rent,riad-rent,villa-rent"
)

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
]

ID_RE = re.compile(r"/a/(\d+)/")
PRICE_NUM_RE = re.compile(r"[\d\s]+")

# ----------------------------
# Helpers
# ----------------------------
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


def normalize_listing_type(mode: str) -> str:
    """Convert mode to standardized listing_type."""
    return "vente" if mode in ("vendre", "acheter") else "location"


def build_cc_url(mode: str, page: int, order: Optional[str], categories: str) -> str:
    """
    Build a CC SERP URL:
      - vendre → /fr/cc/immobilier-a-vendre-all
      - louer  → /fr/cc/immobilier-a-louer-all
    Flags are colon-separated after the base: :o:n :sc:... :p:N
    """
    base_path = "/fr/cc/immobilier-a-vendre-all" if mode == "vendre" else "/fr/cc/immobilier-a-louer-all"
    flags: List[str] = []
    if order:
        flags.append(f"o:{order}")              # e.g., n=newest
    if categories:
        flags.append(f"sc:{categories}")        # comma-separated list
    if page and page > 1:
        flags.append(f"p:{page}")               # page number

    if flags:
        return f"{BASE}{base_path}:{':'.join(flags)}"
    return f"{BASE}{base_path}"


def extract_firstN_links(html: str, limit: int = 25) -> List[str]:
    """
    Extract listing links from a CC SERP page.
    We’re liberal and scan multiple selectors + a final fallback.
    """
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    seen: Set[str] = set()

    # Cards that embed direct link in a 'linkref' (when present)
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

    # General anchors to be safe
    for a in soup.select('a[href*="/fr/a/"]'):
        href = a.get("href", "").strip()
        if href.startswith("/"):
            href = urljoin(BASE, href)
        if href.startswith("http") and "/fr/a/" in href and href not in seen:
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

# ----------------------------
# Parsing a listing page
# ----------------------------
def parse_listing_details(html: str) -> Dict[str, Any]:
    """
    Extract a minimal, clean set of fields from a Mubawab ad page.
    All structured features (Caractéristiques générales + adDetails)
    go into features_main_json (JSON map).
    """
    soup = BeautifulSoup(html, "lxml")
    out: Dict[str, Any] = {}

    # Title
    title_el = soup.select_one("h1.searchTitle")
    out["title"] = clean_text(title_el.get_text()) if title_el else None

    # Single numeric price
    price_el = soup.select_one("h3.orangeTit")
    price_text = clean_text(price_el.get_text()) if price_el else None
    out["price"] = parse_price_number(price_text)  # int or None

    # Location (often "Quartier à Ville")
    loc_el = soup.select_one("h3.greyTit")
    out["location_text"] = clean_text(loc_el.get_text()) if loc_el else None

    # ----------------------------
    # Main characteristics → map
    # ----------------------------
    attr_dict: Dict[str, str] = {}

    # 1) Caractéristiques générales (labels + values)
    for feature in soup.select("div.caractBlockProp div.adMainFeature"):
        label_el = feature.select_one("p.adMainFeatureContentLabel")
        value_el = feature.select_one("p.adMainFeatureContentValue")
        if label_el and value_el:
            k = clean_text(label_el.get_text())
            v = clean_text(value_el.get_text())
            if k and v:
                attr_dict[k] = v

    # 2) Top icons (surface, pièces, chambres, SDB) dans .adDetails
    detail_index = 1
    for span in soup.select("div.adDetails div.adDetailFeature span"):
        txt = clean_text(span.get_text())
        if not txt:
            continue

        key = None
        if "m²" in txt or "m2" in txt or "\u00b2" in txt:
            key = "Surface"
        elif "Pièce" in txt or "Pièces" in txt:
            key = "Pièces"
        elif "Chambre" in txt:
            key = "Chambres"
        elif "Salle de bain" in txt:
            key = "Salles de bain"
        else:
            key = f"Detail_{detail_index}"
            detail_index += 1

        attr_dict[key] = txt

    out["features_main_json"] = json.dumps(attr_dict, ensure_ascii=False) if attr_dict else None

    # ----------------------------
    # Amenities (icons like Terrasse, Ascenseur…) -> JSON array string
    # ----------------------------
    amenities: List[str] = []
    for feat in soup.select("div.adFeatures div.adFeature span"):
        txt = clean_text(feat.get_text())
        if txt:
            amenities.append(txt)
    out["features_amenities_json"] = json.dumps(amenities, ensure_ascii=False) if amenities else None

    # ----------------------------
    # Description (first paragraph in blockProp)
    # ----------------------------
    desc_block = soup.select_one("div.blockProp")
    desc = None
    if desc_block:
        p_tag = desc_block.find("p")
        if p_tag:
            desc = clean_text(p_tag.get_text())
    out["description_text"] = desc

    # ----------------------------
    # Images (gallery + slider) -> JSON array string
    # ----------------------------
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

    # ----------------------------
    # Agency (name + url)
    # ----------------------------
    agency_name = agency_url = None
    business_info = soup.select_one("div.businessInfo")
    if business_info:
        name_el = business_info.select_one("span.businessName")
        if name_el:
            agency_name = clean_text(name_el.get_text())
        link_el = business_info.select_one("a[href]")
        if link_el:
            href = link_el.get("href", "").strip()
            if href.startswith("/"):
                href = urljoin(BASE, href)
            agency_url = href if href.startswith("http") else None

    out["agency_name"] = agency_name
    out["agency_url"] = agency_url

    return out

# ----------------------------
# Kafka helpers
# ----------------------------
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

# ----------------------------
# Crawl flow
# ----------------------------
def fetch_details(session: requests.Session, url: str, i: int, delay: float, listing_type: str) -> Dict[str, Any]:
    """Fetch and parse a single listing, adding listing_type to the payload"""
    data: Dict[str, Any] = {
        "id": get_id_from_url(url),
        "url": url,
        "error": None,
        "listing_type": listing_type,
    }
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


def crawl_cc_serp(
    mode: str,
    num_pages: int,
    per_page: Optional[int],
    serp_delay: float,
    order: str,
    categories: str,
    start_page: int = 1,
) -> List[str]:
    """
    Crawl SERP from start_page for num_pages pages.
    Exemple: start_page=10, num_pages=10 => pages 10..19 (10 pages).
    """
    session = requests.Session()
    all_links: List[str] = []

    current_page = start_page
    for page_offset in range(num_pages):
        url = build_cc_url(mode, current_page, order=order, categories=categories)
        print(f"[SERP] Page {current_page} → {url}")
        resp = session.get(url, headers=pick_headers(current_page), timeout=30)
        resp.raise_for_status()
        links = extract_firstN_links(resp.text, limit=per_page or 25)
        print(f"       +{len(links)} links")
        for u in links:
            if u not in all_links:
                all_links.append(u)

        if page_offset < num_pages - 1:
            current_page += 1
            time.sleep(serp_delay)

    return all_links


def run_job(
    mode: str,
    num_pages: int = 1,
    sink: str = "csv",
    out_csv: str = "mubawab_out.csv",
    kafka_bootstrap: str = KAFKA_BOOTSTRAP_DEFAULT,
    kafka_topic: str = KAFKA_TOPIC_DEFAULT,
    serp_delay: float = 1.3,
    detail_delay: float = 1.3,
    per_page: Optional[int] = 25,
    max_items: Optional[int] = None,
    order: str = "n",
    categories: Optional[str] = None,
    start_page: int = 1,   # NEW
):
    # normalize mode like Avito (support 'acheter' alias)
    mode = "vendre" if mode in ("vendre", "acheter") else "louer"
    listing_type = normalize_listing_type(mode)

    # pick default categories if not provided
    if not categories:
        categories = CC_DEFAULT_CATEGORIES_SALE if mode == "vendre" else CC_DEFAULT_CATEGORIES_RENT

    print(
        f"[*] MUBAWAB CC | Mode={mode} (type={listing_type}) "
        f"| StartPage={start_page} | Pages={num_pages} "
        f"| Order=o:{order} | Categories={categories} "
        f"| Sink={sink} | Limit={max_items} | PerPage={per_page}"
    )

    links = crawl_cc_serp(
        mode,
        num_pages,
        per_page,
        serp_delay,
        order=order,
        categories=categories,
        start_page=start_page,
    )
    if max_items is not None:
        links = links[:max_items]
    print(f"[*] Total links: {len(links)}")

    session = requests.Session()
    rows: List[Dict[str, Any]] = []
    producer = build_producer(kafka_bootstrap) if sink == "kafka" else None

    for i, u in enumerate(links, 1):
        print(f"[{i}/{len(links)}] {u}")
        row = fetch_details(session, u, i, detail_delay, listing_type)
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
                    for k, v in list(r.items()):
                        if isinstance(v, str):
                            r[k] = re.sub(r"\s+", " ", v).strip()
                    w.writerow(r)
        print(f"[✓] Wrote {len(rows)} rows → {out_csv}")
    else:
        print(f"[✓] Sent {len(rows)} messages to Kafka topic '{kafka_topic}'")

# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scrape Mubawab CC SERPs (all categories, newest first).")
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
    ap.add_argument("--order", default="n", help="Mubawab cc order flag (e.g., n=newest)")
    ap.add_argument("--categories", default=None, help="Comma-separated cc categories (override defaults)")

    # NEW: start page
    ap.add_argument("--start-page", type=int, default=1)

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
        order=args.order,
        categories=args.categories,
        start_page=args.start_page,
    )
