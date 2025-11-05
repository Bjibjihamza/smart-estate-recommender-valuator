# mubawab_scraper.py
# Fixed version with corrected selectors for Mubawab
# Usage:
#   python mubawab_scraper.py --mode louer --pages 3 --per-page 10 --out ads_mubawab.csv
#   python mubawab_scraper.py --mode vendre --pages 5 --per-page 15 --delay 1.7

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


def pick_headers(i: int):
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
    t = (
        text.replace("\r", " ")
        .replace("\n", " ")
        .replace("\t", " ")
    )
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def page_url(mode: str, page: int) -> str:
    base = LISTING_VENDRE if mode == "vendre" else LISTING_LOUER
    if page <= 1:
        return base
    return f"{base}:p:{page}"


def extract_firstN_links(html: str, limit: int = 10) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    seen: Set[str] = set()

    for box in soup.select('div.listingBox[linkref]'):
        href = box.get("linkref")
        if href and href.startswith("http") and "/fr/a/" in href:
            if href not in seen:
                seen.add(href)
                links.append(href)
                if len(links) >= limit:
                    return links

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

    for a in soup.select('a[href*="/fr/a/"]'):
        href = a.get("href", "").strip()
        if href.startswith("/"):
            href = urljoin(BASE, href)
        if href.startswith("http"):
            if href not in seen:
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
    except:
        return None


def parse_listing_details(html: str) -> Dict[str, Any]:
    """
    Extract details from an ad page with corrected selectors.
    """
    soup = BeautifulSoup(html, "lxml")
    out: Dict[str, Any] = {}

    # Title - the h1.searchTitle is the main title
    title = None
    title_el = soup.select_one("h1.searchTitle")
    if title_el:
        title = clean_text(title_el.get_text())
    out["title"] = title

    # Price - h3.orangeTit contains the price
    price_text = None
    price_el = soup.select_one("h3.orangeTit")
    if price_el:
        price_text = clean_text(price_el.get_text())
    out["price_text"] = price_text
    out["price_number"] = parse_price_number(price_text)

    # Location - h3.greyTit contains location like "Kouilma à Tétouan"
    location_text = None
    loc_el = soup.select_one("h3.greyTit")
    if loc_el:
        location_text = clean_text(loc_el.get_text())
    out["location_text"] = location_text

    # Ad details (size / pieces / chambres / sdb) - div.adDetails
    ad_details = []
    for feat in soup.select("div.adDetails div.adDetailFeature"):
        txt = clean_text(feat.get_text())
        if txt:
            ad_details.append(txt)
    out["ad_details_text"] = json.dumps(ad_details, ensure_ascii=False) if ad_details else None

    # Amenities from div.adFeatures (icons like Terrasse)
    amenities = []
    for feat in soup.select("div.adFeatures div.adFeature span"):
        txt = clean_text(feat.get_text())
        if txt:
            amenities.append(txt)
    out["features_amenities_json"] = json.dumps(amenities, ensure_ascii=False) if amenities else None

    # Description - in blockProp after h1.searchTitle
    desc = None
    desc_block = soup.select_one("div.blockProp")
    if desc_block:
        # Find the p tag after the title
        p_tag = desc_block.find("p")
        if p_tag:
            desc = clean_text(p_tag.get_text())
    out["description_text"] = desc

    # Main characteristics - from div.adMainFeature inside div.caractBlockProp
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

    # Gallery images - from picturesGallery
    imgs: List[str] = []
    for img in soup.select("div.picturesGallery img[src]"):
        src = img.get("src")
        if src and "mubawab-media.com/ad/" in src and src.endswith(".avif"):
            imgs.append(src)
    # Also check slider
    for img in soup.select("div.flipsnap img[src]"):
        src = img.get("src")
        if src and "mubawab-media.com/ad/" in src and src.endswith(".avif"):
            imgs.append(src)
    
    dedup, seen = [], set()
    for u in imgs:
        if u not in seen:
            seen.add(u)
            dedup.append(u)
    out["gallery_urls"] = json.dumps(dedup, ensure_ascii=False) if dedup else None

    # Agency info - from div.businessInfo or div.agency-info
    agency_name = None
    agency_url = None
    agency_logo = None
    
    # Check businessInfo section
    business_info = soup.select_one("div.businessInfo")
    if business_info:
        # Name from span.businessName
        name_el = business_info.select_one("span.businessName")
        if name_el:
            agency_name = clean_text(name_el.get_text())
        
        # Look for logo
        logo_el = business_info.select_one("img[src]")
        if logo_el:
            agency_logo = logo_el.get("src")
            if agency_logo and agency_logo.startswith("/"):
                agency_logo = urljoin(BASE, agency_logo)
        
        # Look for link
        link_el = business_info.select_one("a[href]")
        if link_el:
            href = link_el.get("href", "").strip()
            if href.startswith("/"):
                href = urljoin(BASE, href)
            agency_url = href if href.startswith("http") else None
    
    out["agency_name"] = agency_name
    out["agency_url"] = agency_url
    out["agency_logo"] = agency_logo

    # Flags
    has_phone_box = True if soup.select_one(".phone-number-box, .contactPhoneClick") else False
    has_map_block = True if soup.select_one(".prop-map-holder, #mapOpen, #mapClosed") else False
    out["has_phone_box"] = has_phone_box
    out["has_map_block"] = has_map_block

    return out


def fetch_details(session: requests.Session, url: str, i: int, delay: float) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "id": get_id_from_url(url),
        "url": url,
        "error": None,
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


def run(mode: str, pages: int, per_page: int, out_csv: str, delay: float):
    session = requests.Session()
    all_rows: List[Dict[str, Any]] = []

    print(f"[*] Mode={mode} | pages={pages} | per_page={per_page} → {out_csv}")

    for p in range(1, pages + 1):
        list_url = page_url(mode, p)
        print(f"[PAGE {p}] {list_url}")
        resp = session.get(list_url, headers=pick_headers(p), timeout=30)
        resp.raise_for_status()
        links = extract_firstN_links(resp.text, per_page)
        print(f"       +{len(links)} liens")

        for i, link in enumerate(links, 1):
            print(f"   [{i}/{len(links)}] {link}")
            row = fetch_details(session, link, i + p, delay)
            row_out = {
                "id": row.get("id"),
                "url": row.get("url"),
                "title": row.get("title"),
                "price_text": row.get("price_text"),
                "price_number": row.get("price_number"),
                "location_text": row.get("location_text"),
                "ad_details_text": row.get("ad_details_text"),
                "features_main_json": row.get("features_main_json"),
                "features_amenities_json": row.get("features_amenities_json"),
                "description_text": row.get("description_text"),
                "gallery_urls": row.get("gallery_urls"),
                "agency_name": row.get("agency_name"),
                "agency_url": row.get("agency_url"),
                "agency_logo": row.get("agency_logo"),
                "has_phone_box": row.get("has_phone_box"),
                "has_map_block": row.get("has_map_block"),
                "error": row.get("error"),
            }
            all_rows.append(row_out)

    if all_rows:
        fieldnames = list(all_rows[0].keys())
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                quoting=csv.QUOTE_ALL,
                extrasaction="ignore",
            )
            w.writeheader()
            for r in all_rows:
                for k, v in list(r.items()):
                    if isinstance(v, str):
                        r[k] = clean_text(v)
                w.writerow(r)
        print(f"[✓] Wrote {len(all_rows)} rows → {out_csv}")
    else:
        print("[!] No rows collected.")


def main():
    ap = argparse.ArgumentParser(description="Scrape Mubawab listings (links + details) into CSV.")
    ap.add_argument("--mode", choices=["louer", "vendre"], default="louer",
                    help="Type d'annonces (louer/vendre). Défaut: louer")
    ap.add_argument("--pages", type=int, default=1,
                    help="Nombre de pages de listing à parcourir. Défaut: 1")
    ap.add_argument("--per-page", type=int, default=25,
                    help="Nombre maximum de liens/annonces par page à traiter. Défaut: 25")
    ap.add_argument("--out", default="ads_mubawab.csv",
                    help="Chemin du CSV de sortie. Défaut: ads_mubawab.csv")
    ap.add_argument("--delay", type=float, default=1.3,
                    help="Délai (s) entre requêtes de DÉTAIL (politesse). Défaut: 1.3")
    args = ap.parse_args()

    run(args.mode, args.pages, args.per_page, args.out, args.delay)


if __name__ == "__main__":
    main()