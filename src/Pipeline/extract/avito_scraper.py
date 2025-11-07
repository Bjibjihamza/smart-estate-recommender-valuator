# avito_scraper.py - batched Kafka, robust parsing, with listing_type
import csv, re, time, json, argparse
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer

# ----------------------------
# Defaults / Constants
# ----------------------------
KAFKA_BOOTSTRAP_DEFAULT = "kafka:9092"
KAFKA_TOPIC_DEFAULT = "realestate.avito.raw"

BASE = "https://www.avito.ma"
START_LOUER = "https://www.avito.ma/fr/maroc/locations_immobilieres-à_louer"
START_VENDRE = "https://www.avito.ma/fr/maroc/ventes_immobilieres-à_vendre"

HEADERS_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

ID_RE = re.compile(r"_(\d+)\.htm")
NUM_RE = re.compile(r"[\d\s,.]+")

# ----------------------------
# Helpers
# ----------------------------
def pick_headers(i: int) -> Dict[str, str]:
    return {
        "User-Agent": HEADERS_LIST[i % len(HEADERS_LIST)],
        "Accept-Language": "fr,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

def clean_text(text: Any) -> Optional[str]:
    if text is None:
        return None
    if not isinstance(text, str):
        text = str(text)
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None

def text_or_none(el) -> Optional[str]:
    if not el:
        return None
    t = el.get_text(" ", strip=True)
    return clean_text(t) if t else None

def get_id_from_url(url: str) -> Optional[str]:
    m = ID_RE.search(url)
    return m.group(1) if m else None

# normalize Avito mode -> listing_type
def normalize_listing_type(mode: str) -> str:
    """
    Standardize the listing type for downstream (Silver) use:
      - 'vendre'/'acheter' -> 'vente'
      - 'louer' -> 'location'
    """
    return "vente" if mode in ("vendre", "acheter") else "location"

# ---------- Convert relative time to absolute date ----------
def parse_relative_date(text: str, scrape_time: datetime) -> Optional[str]:
    """
    Convert 'il y a X minutes/heures/jours/semaines/mois/ans' into absolute ISO datetime string.
    """
    if not text:
        return None

    text_lower = text.lower().strip()
    num_match = re.search(r"(\d+)", text_lower)
    if not num_match:
        return scrape_time.strftime("%Y-%m-%d %H:%M:%S")

    num = int(num_match.group(1))

    if "minute" in text_lower:
        delta = timedelta(minutes=num)
    elif "heure" in text_lower:
        delta = timedelta(hours=num)
    elif "jour" in text_lower:
        delta = timedelta(days=num)
    elif "semaine" in text_lower:
        delta = timedelta(weeks=num)
    elif "mois" in text_lower:
        delta = timedelta(days=num * 30)  # approx
    elif "an" in text_lower or "année" in text_lower:
        delta = timedelta(days=num * 365)  # approx
    else:
        return scrape_time.strftime("%Y-%m-%d %H:%M:%S")

    absolute_date = scrape_time - delta
    return absolute_date.strftime("%Y-%m-%d %H:%M:%S")

# ----------------------------
# SERP
# ----------------------------
def extract_listings_from_serp(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    out = []

    # Primary card links
    for a in soup.select('a.sc-1jge648-0[href]'):
        href = a.get("href")
        if href and "_" in href and ".htm" in href:
            out.append(urljoin(BASE, href.strip()))

    # Some pages use section container
    for a in soup.select('section.sc-b341c660-0 a[href]'):
        href = a.get("href")
        if href and "_" in href and ".htm" in href:
            out.append(urljoin(BASE, href.strip()))

    # dedup
    seen, urls = set(), []
    for u in out:
        if u not in seen:
            seen.add(u)
            urls.append(u)
    return urls

def find_next_page(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    # UI chevron next
    for a in soup.select('a.sc-1cf7u6r-0[href]'):
        svg = a.select_one('svg[aria-labelledby*="ChevronRight"]')
        if svg:
            return urljoin(BASE, a["href"])

    # Fallback on 'o=' param
    u = urlparse(current_url)
    qs = parse_qs(u.query)
    if "o" in qs:
        try:
            n = int(qs["o"][0])
            qs["o"] = [str(n + 1)]
            new_q = urlencode({k: (v[0] if len(v) == 1 else v) for k, v in qs.items()}, doseq=True)
            return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
        except Exception:
            pass
    else:
        return f"{current_url}?o=2"

    return None

def crawl_serp(start_url: str, max_pages: int, delay: float, max_items: Optional[int] = None) -> List[str]:
    session = requests.Session()
    urls, url = [], start_url
    for page in range(1, max_pages + 1):
        print(f"[SERP] Page {page} → {url}")
        r = session.get(url, headers=pick_headers(page), timeout=30)
        r.raise_for_status()
        found = extract_listings_from_serp(r.text)
        for u in found:
            if u not in urls:
                urls.append(u)
                if max_items is not None and len(urls) >= max_items:
                    print(f"       Reached limit {max_items}, stopping SERP crawl")
                    return urls
        print(f"       +{len(found)} found (unique total {len(urls)})")
        if page < max_pages:
            nxt = find_next_page(r.text, url)
            if not nxt or nxt == url:
                print("       No next page found, stopping")
                break
            url = nxt
            time.sleep(delay)
    return urls

# ----------------------------
# attributes -> JSON
# ----------------------------
def extract_attributes_json(soup: BeautifulSoup) -> Dict[str, Any]:
    attributes: Dict[str, Any] = {}
    # Label/value pairs (selectors may evolve)
    for item in soup.select("div.sc-cd1c365e-1.clDxnX"):
        img = item.select_one("img")
        value_el = item.select_one("span.sc-1x0vz2r-0.fjZBup")
        label_el = item.select_one("span.sc-1x0vz2r-0.bXFCIH")
        if img and "alt" in img.attrs and label_el:
            label = clean_text(label_el.get_text())
            value = clean_text(value_el.get_text()) if value_el else None
            if label and value:
                attributes[label] = value
    return attributes

# ----------------------------
# details
# ----------------------------
def extract_from_dom(soup: BeautifulSoup, scrape_time: datetime) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    out["title"] = text_or_none(soup.select_one("h1.sc-9ca53b09-5"))
    out["price_text"] = text_or_none(soup.select_one("p.sc-9ca53b09-10"))

    # breadcrumbs
    breadcrumbs = []
    for a in soup.select("ol.sc-16q833i-0 a, ol.sc-16q833i-0 span.sc-16q833i-2"):
        txt = text_or_none(a)
        if txt:
            breadcrumbs.append(txt)
    out["breadcrumbs"] = " > ".join(breadcrumbs) if breadcrumbs else None

    out["category"] = text_or_none(soup.select_one("div.sc-172bdcdc-0 span.sc-1x0vz2r-0.fjZBup"))
    out["description"] = text_or_none(soup.select_one("div.sc-7378144c-0"))

    attrs = extract_attributes_json(soup)
    out["attributes"] = json.dumps(attrs, ensure_ascii=False) if attrs else None

    # equipments/features list
    equipments = []
    for item in soup.select("div.sc-cd1c365e-0.bERAxq div.sc-cd1c365e-1"):
        txt = text_or_none(item.select_one("span.sc-1x0vz2r-0.fjZBup"))
        if txt:
            equipments.append(txt)
    out["equipments"] = "; ".join(equipments) if equipments else None

    out["seller_name"] = text_or_none(soup.select_one("p.sc-1l0do2b-9"))
    out["seller_type"] = "Boutique" if soup.select_one('svg[aria-labelledby*="ShopBadge"]') else "Particulier"

    # published_date (absolute from relative)
    time_el = soup.select_one("time")
    if time_el:
        relative_text = text_or_none(time_el.parent)
        out["published_date"] = parse_relative_date(relative_text, scrape_time)
    else:
        out["published_date"] = None

    # images
    imgs = []
    for img in soup.select("div.slick-slide img[src]"):
        src = img.get("src")
        if src and "content.avito.ma" in src:
            imgs.append(src)
    dedup, seen = [], set()
    for u in imgs:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    out["image_urls"] = " | ".join(dedup) if dedup else None

    return out

def parse_property(url: str, i: int, listing_type: str) -> Dict[str, Any]:
    """
    Fetch and parse a single listing detail page.
    listing_type is injected from the run_job's normalized mode ('vente'/'location').
    """
    scrape_time = datetime.now()
    out: Dict[str, Any] = {
        "id": get_id_from_url(url),
        "url": url,
        "error": None,
        "listing_type": listing_type,  # NEW
    }
    try:
        resp = requests.get(url, headers=pick_headers(i), timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        out.update(extract_from_dom(soup, scrape_time))
    except Exception as e:
        out["error"] = str(e)
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
    key = str(row.get("id", "")).encode("utf-8")
    value = json.dumps(row, ensure_ascii=False).encode("utf-8")
    producer.produce(kafka_topic, key=key, value=value, on_delivery=delivery_cb)
    producer.poll(0)  # non-blocking; serves delivery callbacks

# ----------------------------
# job
# ----------------------------
def run_job(
    mode: str,
    num_pages: int = 1,
    sink: str = "kafka",
    out_csv: str = "avito_out.csv",
    kafka_bootstrap: str = KAFKA_BOOTSTRAP_DEFAULT,
    kafka_topic: str = KAFKA_TOPIC_DEFAULT,
    serp_delay: float = 1.5,
    detail_delay: float = 1.5,
    max_items: Optional[int] = None,
):
    # normalize mode and compute listing_type
    mode = "vendre" if mode in ("vendre", "acheter") else "louer"
    start = START_VENDRE if mode == "vendre" else START_LOUER
    listing_type = normalize_listing_type(mode)  # NEW

    print(
        f"[*] Mode: {mode.upper()} (type={listing_type}) | Pages: {num_pages} | Sink: {sink} | Limit: {max_items}"
    )

    urls = crawl_serp(start, num_pages, serp_delay, max_items=max_items)
    if max_items is not None:
        urls = urls[:max_items]
    print(f"[*] Total URLs: {len(urls)}")

    rows: List[Dict[str, Any]] = []
    producer = build_producer(kafka_bootstrap) if sink == "kafka" else None

    for i, u in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] {u}")
        row = parse_property(u, i, listing_type)  # NEW: pass listing_type
        rows.append(row)
        if producer:
            send_to_kafka(row, producer, kafka_topic)
        if max_items is not None and i >= max_items:
            break
        time.sleep(detail_delay)

    if producer:
        producer.flush(10)

    if sink == "csv":
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            if rows:
                fieldnames = list(rows[0].keys())
                w = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    extrasaction="ignore",
                    quoting=csv.QUOTE_ALL,
                )
                w.writeheader()
                for r in rows:
                    w.writerow(r)
        print(f"[✓] Wrote {len(rows)} rows → {out_csv}")
    else:
        print(f"[✓] Sent {len(rows)} messages to Kafka topic '{kafka_topic}'")

# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["louer", "vendre", "acheter"], default="louer")
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sink", choices=["kafka", "csv"], default="kafka")
    ap.add_argument("--out", default="avito_out.csv")
    ap.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP_DEFAULT)
    ap.add_argument("--topic", default=KAFKA_TOPIC_DEFAULT)
    ap.add_argument("--serp-delay", type=float, default=1.5)
    ap.add_argument("--detail-delay", type=float, default=1.5)
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
        max_items=args.limit,
    )
