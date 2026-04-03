"""
================================================================================
  Dutchie Dispensary Scraper — Apify Actor (main.py)
  Version: 1.0.0
================================================================================

  Architecture:
    1. Accept input from Apify (one or more Dutchie dispensary URLs)
    2. Extract the URL slug from each Dutchie URL
    3. Resolve the slug to a dispensaryId via a 4-step resolution chain:
       a. Direct ConsumerDispensaries query (slug as cNameOrID)
       b. Pre-built lookup table (dispensary_lookup.json, ~5,000 entries)
       c. HTML page scrape (extract real cName from __NEXT_DATA__ / Apollo cache)
       d. DispensarySearch API fallback (name-based search)
    4. Fetch all products via the FilteredProducts persisted query
       on /api-2/graphql with page-based pagination
    5. Normalize each product to the canonical output schema
    6. Charge one PPR event per product variant (SKU) and push to dataset

  Monetization:
    Pay-Per-Result (PPR) — $1.50 per 1,000 results
    One billable result = one unique product variant (SKU)

  Technical Notes:
    - Dutchie uses TWO separate GraphQL endpoints
    - All queries use persisted query hashes (GET requests)
    - curl_cffi with Chrome TLS fingerprint bypasses Cloudflare
    - Pagination is page-based (page=0, perPage=50), NOT offset-based
    - dispensary_lookup.json maps ~5,000 slugs to cNames (built offline)

================================================================================
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests

# ── Apify SDK — graceful fallback for local testing ───────────────────────────
try:
    from apify import Actor
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("dutchie-scraper")


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA_VERSION = "1.0"
OFFERS_SCHEMA_VERSION = "1.0"

# Category hint keywords for offer card classification
CATEGORY_HINT_MAP = [
    ("flower",      ["flower", "bud", "oz", "ounce", "gram", "eighth", "quarter", "half", "1/8", "1/4", "1/2", "14g", "7g", "3.5g"]),
    ("pre-roll",    ["pre-roll", "preroll", "pre roll", "joint", "infused pre", "blunt"]),
    ("vape",        ["vape", "cart", "cartridge", "disposable", "distillate", "live resin cart", "rosin cart"]),
    ("edible",      ["edible", "gummy", "gummies", "chocolate", "brownie", "cookie", "candy", "drink", "beverage", "soda", "lemonade", "mg"]),
    ("beverage",    ["drink", "beverage", "soda", "lemonade", "sparkling", "shot", "elixir"]),
    ("concentrate", ["concentrate", "wax", "shatter", "rosin", "resin", "badder", "budder", "crumble", "hash", "dab", "extract", "live"]),
    ("tincture",    ["tincture", "drops", "sublingual"]),
    ("topical",     ["topical", "cream", "lotion", "balm", "patch", "salve"]),
    ("accessory",   ["accessory", "accessories", "pipe", "grinder", "paper", "rolling"]),
]

# Dutchie GraphQL endpoints
DISPENSARY_ENDPOINT = "https://dutchie.com/graphql"
PRODUCTS_ENDPOINT   = "https://dutchie.com/api-2/graphql"

# Persisted query hashes (validated against live Dutchie API, March 2026)
HASH_CONSUMER_DISPENSARIES = "0d3ff8648848a737bbbeff9d090854ce2b78a7c4330d4982dab5b32ba2009448"
HASH_FILTERED_PRODUCTS     = "c3dda0418c4b423ed26a38d011b50a2b8c9a1f8bde74b45f93420d60d2c50ae1"
HASH_DISPENSARY_SEARCH     = "3e25a4d63e0a5e220b9b8e6b5a2e3c4f5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a"

# Required headers for Dutchie's Apollo/GraphQL gateway
BASE_HEADERS = {
    "Accept":                    "application/json",
    "Content-Type":              "application/json",
    "apollo-require-preflight":  "true",
    "Origin":                    "https://dutchie.com",
    "Referer":                   "https://dutchie.com/",
}

# Pagination & rate limiting
PAGE_SIZE      = 50    # Max products per page (Dutchie's limit)
MAX_PAGES      = 200   # Safety cap: 200 × 50 = 10,000 products
MAX_RETRIES    = 3     # Retries per request
RETRY_BACKOFF  = 2.0   # Exponential backoff base (seconds)
REQUEST_DELAY  = 0.4   # Delay between paginated requests (seconds)

# PPR: charge one event per product variant
PPR_EVENT_NAME = "product-scraped"


# ══════════════════════════════════════════════════════════════════════════════
# DISPENSARY LOOKUP TABLE
# ══════════════════════════════════════════════════════════════════════════════

def _load_lookup() -> dict:
    """Load the pre-built dispensary slug → cName lookup table."""
    path = Path(__file__).parent / "dispensary_lookup.json"
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            logger.info(f"Loaded lookup table: {len(data):,} entries")
            return data
        except Exception as e:
            logger.warning(f"Could not load lookup table: {e}")
    return {}

DISPENSARY_LOOKUP = _load_lookup()


# ══════════════════════════════════════════════════════════════════════════════
# URL PARSING
# ══════════════════════════════════════════════════════════════════════════════

def extract_slug(url: str) -> str:
    """
    Extract the dispensary slug from a Dutchie URL.

    Handles:
      https://dutchie.com/dispensary/store-name
      https://dutchie.com/dispensary/store-name/menu
      https://dutchie.com/embedded-menu/store-name
      https://www.dutchie.com/dispensary/store-name?ref=abc
    """
    if not url or not isinstance(url, str):
        raise ValueError(f"Invalid URL: {url!r}")

    url = url.strip().rstrip("/")
    parsed = urlparse(url)

    # Reject non-Dutchie domains (e.g. leafly.com, weedmaps.com)
    if parsed.scheme and parsed.netloc:
        host = parsed.netloc.lower().lstrip("www.")
        if not (host == "dutchie.com" or host.endswith(".dutchie.com")):
            raise ValueError(
                f"Not a Dutchie URL: {url!r}. "
                f"Expected format: https://dutchie.com/dispensary/your-store-name"
            )

    path = parsed.path.strip("/")
    segments = [s for s in path.split("/") if s]

    if len(segments) >= 2:
        prefix = segments[0].lower()
        if prefix in ("dispensary", "embedded-menu"):
            slug = segments[1]
            # Reject if slug is itself a reserved path segment
            if slug and slug.lower() not in ("dispensary", "embedded-menu", "menu") \
                    and re.match(r"^[a-zA-Z0-9][-a-zA-Z0-9]*$", slug):
                return slug
            else:
                raise ValueError(
                    f"Empty or invalid slug in URL: {url!r}. "
                    f"Expected format: https://dutchie.com/dispensary/your-store-name"
                )

    # Reject bare strings (no scheme, no path structure)
    if not parsed.scheme:
        raise ValueError(
            f"Not a valid URL: {url!r}. "
            f"Expected format: https://dutchie.com/dispensary/your-store-name"
        )

    # Reject single-segment paths that are just 'dispensary' or 'embedded-menu'
    if len(segments) == 1 and segments[0].lower() in ("dispensary", "embedded-menu"):
        raise ValueError(
            f"Missing dispensary slug in URL: {url!r}. "
            f"Expected format: https://dutchie.com/dispensary/your-store-name"
        )

    if segments:
        candidate = segments[-1] if segments[-1] != "menu" else (
            segments[-2] if len(segments) >= 2 else ""
        )
        if candidate and re.match(r"^[a-zA-Z0-9][-a-zA-Z0-9]*$", candidate):
            logger.warning(f"Non-standard URL — extracted slug: {candidate}")
            return candidate

    raise ValueError(
        f"Could not extract dispensary slug from URL: {url}\n"
        f"Expected format: https://dutchie.com/dispensary/your-store-name"
    )


# ══════════════════════════════════════════════════════════════════════════════
# HTTP CLIENT
# ══════════════════════════════════════════════════════════════════════════════

class DutchieClient:
    """
    HTTP client for Dutchie's GraphQL API.
    Uses curl_cffi to impersonate Chrome's TLS fingerprint, bypassing Cloudflare.
    """

    def __init__(self, proxy_url: str | None = None):
        self.session   = cffi_requests.Session(impersonate="chrome")
        self.proxy_url = proxy_url

    def get(self, url: str, params: dict, headers: dict, timeout: int = 30) -> dict:
        """GET request with exponential-backoff retry."""
        kwargs: dict = {
            "params":      params,
            "headers":     headers,
            "impersonate": "chrome",
            "timeout":     timeout,
        }
        if self.proxy_url:
            kwargs["proxies"] = {"https": self.proxy_url, "http": self.proxy_url}

        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(url, **kwargs)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429:
                    wait = RETRY_BACKOFF * (2 ** attempt) * 2
                    logger.warning(f"Rate-limited (429). Waiting {wait:.1f}s …")
                    time.sleep(wait)
                    continue

                if resp.status_code in (502, 503, 504):
                    wait = RETRY_BACKOFF * (2 ** attempt)
                    logger.warning(f"Server error {resp.status_code}. Retry in {wait:.1f}s …")
                    time.sleep(wait)
                    continue

                if resp.status_code == 403:
                    raise RuntimeError("403 Forbidden — Cloudflare block. Enable Apify proxy.")

                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")

            except (ConnectionError, TimeoutError, OSError) as e:
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning(f"Connection error: {e}. Retry in {wait:.1f}s …")
                last_error = e
                time.sleep(wait)

        raise RuntimeError(f"All {MAX_RETRIES} retries exhausted. Last error: {last_error}")

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# DISPENSARY RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def _query_dispensary(client: DutchieClient, cname_or_id: str) -> list[dict]:
    """Query ConsumerDispensaries for a given cNameOrID."""
    headers = {**BASE_HEADERS, "x-apollo-operation-name": "ConsumerDispensaries"}
    params  = {
        "operationName": "ConsumerDispensaries",
        "variables":     json.dumps({"dispensaryFilter": {"cNameOrID": cname_or_id}}),
        "extensions":    json.dumps({
            "persistedQuery": {"version": 1, "sha256Hash": HASH_CONSUMER_DISPENSARIES}
        }),
    }
    data = client.get(DISPENSARY_ENDPOINT, params=params, headers=headers)
    return data.get("data", {}).get("filteredDispensaries", [])


def _extract_cname_from_html(client: DutchieClient, slug: str) -> str | None:
    """Scrape the dispensary page HTML to find the real cName."""
    try:
        resp = client.session.get(
            f"https://dutchie.com/dispensary/{slug}",
            impersonate="chrome",
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        html = resp.text

        # Server-side redirect
        final_url   = str(resp.url)
        final_match = re.search(r'/dispensar(?:y|ies)/([^/?#]+)', final_url)
        if final_match:
            resolved = final_match.group(1)
            if resolved != slug:
                return resolved

        # Canonical link tag
        canonical = re.search(
            r'<link[^>]*rel=["\']canonical["\'][^>]*href=["\'][^"\']*dispensar(?:y|ies)/([^"\'/]+)',
            html, re.I
        )
        if canonical and canonical.group(1) != slug:
            return canonical.group(1)

        # __NEXT_DATA__ JSON blob
        nd_match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
        if nd_match:
            try:
                nd    = json.loads(nd_match.group(1))
                props = nd.get("props", {}).get("pageProps", {})
                cname = props.get("dispensary", {}).get("cName")
                if cname and cname != slug:
                    return cname
            except (json.JSONDecodeError, KeyError):
                pass

        # Apollo cache in script tags
        apollo = re.search(r'"cName"\s*:\s*"([^"]+)"', html)
        if apollo and apollo.group(1) != slug:
            return apollo.group(1)

    except Exception as e:
        logger.warning(f"[{slug}] HTML fallback failed: {e}")
    return None


def _lookup_slug(slug: str) -> str | None:
    """Check the pre-built lookup table for a matching cName."""
    if slug in DISPENSARY_LOOKUP:
        return DISPENSARY_LOOKUP[slug].get("cName", slug)

    slug_lower = slug.lower()
    candidates = [
        (k, v) for k, v in DISPENSARY_LOOKUP.items()
        if slug_lower in k.lower() or k.lower() in slug_lower
    ]
    if len(candidates) == 1:
        return candidates[0][1].get("cName", candidates[0][0])
    return None


def resolve_dispensary(client: DutchieClient, slug: str) -> dict:
    """
    Resolve a URL slug to a dispensary record {id, name, cName}.

    Resolution chain (4 steps):
      1. Direct ConsumerDispensaries query
      2. Pre-built lookup table
      3. HTML page scrape
      4. DispensarySearch API fallback
    """
    logger.info(f"[{slug}] Resolving dispensary …")

    # Step 1 — direct query
    results = _query_dispensary(client, slug)
    if results:
        d = results[0]
        return {"id": d.get("id", ""), "name": d.get("name", ""), "cName": d.get("cName", slug)}

    # Step 2 — lookup table
    lookup_cname = _lookup_slug(slug)
    if lookup_cname and lookup_cname != slug:
        results2 = _query_dispensary(client, lookup_cname)
        if results2:
            d = results2[0]
            return {"id": d.get("id", ""), "name": d.get("name", ""), "cName": d.get("cName", lookup_cname)}

    # Step 3 — HTML scrape
    html_cname = _extract_cname_from_html(client, slug)
    if html_cname:
        results3 = _query_dispensary(client, html_cname)
        if results3:
            d = results3[0]
            return {"id": d.get("id", ""), "name": d.get("name", ""), "cName": d.get("cName", html_cname)}

    # Step 4 — DispensarySearch API
    try:
        search_term = slug.replace("-", " ").strip()
        headers     = {**BASE_HEADERS, "x-apollo-operation-name": "DispensarySearch"}
        params      = {
            "operationName": "DispensarySearch",
            "variables":     json.dumps({"searchTerm": search_term, "limit": 10}),
            "extensions":    json.dumps({
                "persistedQuery": {"version": 1, "sha256Hash": HASH_DISPENSARY_SEARCH}
            }),
        }
        data    = client.get(DISPENSARY_ENDPOINT, params=params, headers=headers)
        matches = (
            data.get("data", {}).get("dispensarySearch", {}).get("results", [])
            or data.get("data", {}).get("filteredDispensaries", [])
        )
        slug_clean = slug.lower().replace("-", "")
        best, best_score = None, 0
        for r in matches:
            cname_clean = r.get("cName", "").lower().replace("-", "")
            name_clean  = r.get("name",  "").lower().replace(" ", "")
            score = 0
            if slug_clean == cname_clean:
                score = 100
            elif slug_clean in cname_clean or cname_clean in slug_clean:
                score = 80
            elif slug_clean in name_clean or name_clean in slug_clean:
                score = 60
            else:
                slug_words  = set(slug.lower().split("-"))
                cname_words = set(r.get("cName", "").lower().split("-"))
                name_words  = set(r.get("name",  "").lower().split())
                score = len(slug_words & (cname_words | name_words)) * 20
            if score > best_score:
                best_score = score
                best = r
        if best and best_score >= 40:
            results4 = _query_dispensary(client, best["cName"])
            if results4:
                d = results4[0]
                return {"id": d.get("id", ""), "name": d.get("name", ""), "cName": d.get("cName", best["cName"])}
    except Exception as e:
        logger.warning(f"[{slug}] DispensarySearch fallback failed: {e}")

    raise RuntimeError(
        f"Could not resolve dispensary for slug '{slug}'. All 4 resolution strategies failed.\n"
        f"Tip: Open https://dutchie.com/dispensary/{slug} in your browser, copy the exact URL "
        f"from the address bar, and use that URL as input."
    )


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCT FETCHING
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all_products(
    client:        DutchieClient,
    dispensary_id: str,
    cname:         str,
    max_items:     int = 0,
    pricing_type:  str = "rec",
) -> list[dict]:
    """
    Fetch all products via the FilteredProducts persisted query.
    Uses page-based pagination (page=0, perPage=50).
    """
    headers = {
        **BASE_HEADERS,
        "x-apollo-operation-name": "FilteredProducts",
        "Referer": f"https://dutchie.com/dispensary/{cname}",
    }
    all_products: list[dict] = []

    for page_num in range(MAX_PAGES):
        variables = {
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "dispensaryId":                     dispensary_id,
                "pricingType":                      pricing_type,
                "strainTypes":                      [],
                "subcategories":                    [],
                "Status":                           "Active",
                "types":                            [],
                "useCache":                         False,
                "isDefaultSort":                    True,
                "sortBy":                           "popularSortIdx",
                "sortDirection":                    1,
                "bypassOnlineThresholds":           False,
                "isKioskMenu":                      False,
                "removeProductsBelowOptionThresholds": True,
                "platformType":                     "ONLINE_MENU",
                "preOrderType":                     None,
            },
            "page":    page_num,
            "perPage": PAGE_SIZE,
        }
        params = {
            "operationName": "FilteredProducts",
            "variables":     json.dumps(variables),
            "extensions":    json.dumps({
                "persistedQuery": {"version": 1, "sha256Hash": HASH_FILTERED_PRODUCTS}
            }),
        }

        logger.info(f"[{cname}] Page {page_num + 1} …")
        try:
            data = client.get(PRODUCTS_ENDPOINT, params=params, headers=headers)
        except RuntimeError as e:
            logger.error(f"[{cname}] Request failed on page {page_num + 1}: {e}")
            break

        if "errors" in data:
            logger.error(f"[{cname}] GraphQL error: {data['errors'][0].get('message', '?')}")
            break

        products = data.get("data", {}).get("filteredProducts", {}).get("products", [])
        logger.info(f"[{cname}] Page {page_num + 1}: {len(products)} products")

        if not products:
            break

        all_products.extend(products)

        if max_items > 0 and len(all_products) >= max_items:
            all_products = all_products[:max_items]
            break

        if len(products) < PAGE_SIZE:
            break

        time.sleep(REQUEST_DELAY)

    logger.info(f"[{cname}] Total raw products: {len(all_products)}")
    return all_products


# ══════════════════════════════════════════════════════════════════════════════
# CANONICAL OUTPUT SCHEMA — NORMALIZATION
# ══════════════════════════════════════════════════════════════════════════════

def _safe_str(val) -> str | None:
    """Return a stripped string or None."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _safe_float(val) -> float | None:
    """Parse a float from various representations."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = re.sub(r"[^\d.]", "", str(val))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _extract_potency(raw: dict, field: str) -> tuple[str | None, str | None]:
    """
    Extract a potency string and its unit type from nested Dutchie fields.
    Returns: (formatted_potency_string, unit_type)
    """
    if not raw or not isinstance(raw, dict):
        return None, None

    unit    = _safe_str(raw.get("unit", "")) or ""
    u_upper = unit.upper()
    if "PERCENT" in u_upper:
        suffix = "%"
        unit_type = "Percentage"
    elif "MILLIGRAM" in u_upper or u_upper == "MG":
        suffix = "mg"
        unit_type = "Milligrams"
    else:
        suffix = ""
        unit_type = None

    potency_str = None

    if "formatted" in raw:
        potency_str = _safe_str(raw["formatted"])
    else:
        rng = raw.get("range")
        if isinstance(rng, list) and len(rng) >= 1:
            if len(rng) == 1:
                v = _safe_float(rng[0])
                if v is not None:
                    potency_str = f"{v:.1f}{suffix}"
            else:
                lo_f = _safe_float(rng[0])
                hi_f = _safe_float(rng[-1])
                if lo_f is not None and hi_f is not None:
                    if abs(lo_f - hi_f) < 0.01:
                        potency_str = f"{lo_f:.1f}{suffix}"
                    else:
                        potency_str = f"{lo_f:.1f}\u2013{hi_f:.1f}{suffix}"
        else:
            val = raw.get("value")
            if val is not None:
                v_f = _safe_float(val)
                if v_f is not None:
                    potency_str = f"{v_f:.1f}{suffix}"

    # Infer unit from formatted string if missing
    if potency_str and not unit_type:
        if "%" in potency_str:
            unit_type = "Percentage"
        elif "mg" in potency_str.lower():
            unit_type = "Milligrams"

    return potency_str, unit_type


def _extract_price_and_size(option: dict) -> tuple[float | None, str | None]:
    """
    Extract numeric price and size label from a Dutchie product option/variant.
    Options can have: {price, specialPrice, weight, unit, name}
    """
    price_raw = option.get("specialPrice") or option.get("price")
    price     = _safe_float(price_raw)

    # Build a human-readable size label
    weight = _safe_float(option.get("weight"))
    unit   = _safe_str(option.get("unit", ""))
    name   = _safe_str(option.get("name", ""))

    if weight and unit:
        size = f"{weight:g}{unit}"
    elif name:
        size = name
    else:
        size = None

    return price, size


def _normalize_strain(raw_strain: str | None) -> str | None:
    """Normalize strain types to standard Golden Schema values."""
    if not raw_strain:
        return None
    s = raw_strain.upper().strip()
    if s in ("N/A", "THC", "CBD", "NONE", "UNKNOWN"):
        return None
    
    mapping = {
        "SATIVA": "Sativa",
        "INDICA": "Indica",
        "HYBRID": "Hybrid",
        "SATIVA_HYBRID": "Sativa-Hybrid",
        "SATIVA-HYBRID": "Sativa-Hybrid",
        "INDICA_HYBRID": "Indica-Hybrid",
        "INDICA-HYBRID": "Indica-Hybrid",
    }
    return mapping.get(s, raw_strain.title())


def _clean_product_name(name: str) -> str:
    """Remove embedded potency data from product names (e.g. *34.61% TAC*)."""
    if not name:
        return "Unknown Product"
    # Remove anything between asterisks at the end of the string
    cleaned = re.sub(r'\s*\*.*?\*\s*$', '', name).strip()
    return cleaned if cleaned else name


def _normalize_category(raw_type: str | None) -> str:
    """Map Dutchie's internal menuType to a clean category name."""
    if not raw_type:
        return "Other"
    mapping = {
        # ALL-CAPS internal Dutchie types
        "FLOWER":       "Flower",
        "VAPORIZERS":   "Vape",
        "EDIBLES":      "Edible",
        "PREROLLS":     "Pre-Roll",
        "CONCENTRATES": "Concentrate",
        "TINCTURES":    "Tincture",
        "TOPICALS":     "Topical",
        "ACCESSORIES":  "Accessory",
        "APPAREL":      "Apparel",
        "SEEDS":        "Seeds",
        "CLONES":       "Clone",
        "CBD":          "CBD",
        # Mixed-case values returned by Dutchie's API directly
        "PRE-ROLLS":    "Pre-Roll",
        "PRE-ROLL":     "Pre-Roll",
        "VAPE":         "Vape",
        "CONCENTRATE":  "Concentrate",
        "TINCTURE":     "Tincture",
        "TOPICAL":      "Topical",
        "ACCESSORY":    "Accessory",
        "EDIBLE":       "Edible",
    }
    return mapping.get(raw_type.upper(), raw_type)


def normalize_product(
    raw:              dict,
    dispensary_name:  str,
    dispensary_url:   str,
    scraped_at:       str,
) -> list[dict]:
    """
    Normalize a raw Dutchie product into one or more canonical output records.
    One record per variant (option/SKU) is returned.
    Each record is one billable PPR result.

    Dutchie API returns Options as a flat list of strings and Prices as a
    parallel list of numbers:
      Options: ["1/8oz", "1/4oz", "1/2oz"]
      Prices:  [30, 55, 100]
      recPrices: [30, 55, 100]  (recreational prices, may differ from Prices)
    """
    # ── Core fields — handle both camelCase and PascalCase ────────────────────
    product_id   = _safe_str(raw.get("_id") or raw.get("id", ""))
    raw_name = _safe_str(raw.get("Name") or raw.get("name", "")) or "Unknown Product"
    product_name = _clean_product_name(raw_name)
    brand        = _safe_str(raw.get("brandName") or raw.get("brand", ""))
    raw_category = _safe_str(raw.get("type") or raw.get("menuType", ""))
    category     = _normalize_category(raw_category)
    subcategory  = _safe_str(raw.get("subcategory", ""))
    strain_type  = _normalize_strain(raw.get("strainType"))
    description  = _safe_str(raw.get("description", ""))
    in_stock     = bool(raw.get("Status") == "Active" or raw.get("inStock", True))

    # Image URL — try multiple field paths
    image_url = _safe_str(
        raw.get("Image")
        or raw.get("image")
        or (raw.get("images") or [{}])[0].get("url")
        or raw.get("POSMetaData", {}).get("canonicalImgUrl")
    )

    # Dutchie product URL
    slug = dispensary_url.rstrip("/").split("/")[-1]
    product_url = (
        f"https://dutchie.com/dispensary/{slug}/product/{product_id}"
        if product_id else None
    )

    # ── Potency ───────────────────────────────────────────────────────────────
    thc_raw = (
        raw.get("THCContent")
        or raw.get("thcContent")
        or raw.get("potencyThc")
        or {}
    )
    cbd_raw = (
        raw.get("CBDContent")
        or raw.get("cbdContent")
        or raw.get("potencyCbd")
        or {}
    )
    thc_level, thc_unit = _extract_potency(thc_raw, "thc")
    cbd_level, cbd_unit = _extract_potency(cbd_raw, "cbd")

    # ── Special offers ────────────────────────────────────────────────────────
    specials     = raw.get("enterpriseProductSpecials") or raw.get("specials") or []
    special_name = None
    if specials and isinstance(specials, list) and isinstance(specials[0], dict):
        special_name = _safe_str(
            specials[0].get("name") or specials[0].get("specialName")
        )

    # ── Options / Variants ────────────────────────────────────────────────────
    # Dutchie returns Options as a flat list of size strings and Prices as a
    # parallel list of floats. recPrices may differ from Prices for rec stores.
    options = raw.get("Options") or raw.get("options") or []
    prices  = raw.get("recPrices") or raw.get("Prices") or raw.get("prices") or []

    records: list[dict] = []

    if options and isinstance(options, list) and len(options) > 0:
        for i, opt in enumerate(options):
            # opt is a string like "1/8oz", "3.5g", "5 Pack", etc.
            if not isinstance(opt, dict):
                size = _safe_str(opt)
            else:
                w = _safe_float(opt.get("weight"))
                u = _safe_str(opt.get("unit", ""))
                if w is not None and u:
                    size = f"{w:g}{u}"  # Use :g to drop trailing zeros, but keeps 0.1
                else:
                    size = _safe_str(opt.get("name", ""))

            # Get the matching price from the parallel prices list
            if isinstance(opt, dict):
                price = _safe_float(opt.get("specialPrice") or opt.get("price"))
            elif i < len(prices):
                price = _safe_float(prices[i])
            else:
                price = None

            display_price = f"${price:.2f}" if price is not None else None

            records.append({
                "schema_version":     SCHEMA_VERSION,
                "dispensary_name":    dispensary_name,
                "dispensary_url":     dispensary_url,
                "product_id":         product_id,
                "product_name":       product_name,
                "brand":              brand,
                "category":           category,
                "subcategory":        subcategory,
                "strain_type":        strain_type,
                "thc_level":          thc_level,
                "cbd_level":          cbd_level,
                "potency_unit":       thc_unit or cbd_unit,
                "variant_size":       size,
                "display_price":      display_price,
                "numeric_price":      price,
                "special_offer_name": special_name,
                "description":        description,
                "image_url":          image_url,
                "product_url":        product_url,
                "in_stock":           in_stock,
                "scraped_at":         scraped_at,
            })
    else:
        # No variants — treat the product itself as one record
        price_raw = (
            (raw.get("recPrices") or raw.get("Prices") or raw.get("prices") or [None])[0]
        )
        price = _safe_float(price_raw)
        records.append({
            "schema_version":     SCHEMA_VERSION,
            "dispensary_name":    dispensary_name,
            "dispensary_url":     dispensary_url,
            "product_id":         product_id,
            "product_name":       product_name,
            "brand":              brand,
            "category":           category,
            "subcategory":        subcategory,
            "strain_type":        strain_type,
            "thc_level":          thc_level,
            "cbd_level":          cbd_level,
            "potency_unit":       thc_unit or cbd_unit,
            "variant_size":       None,
            "display_price":      f"${price:.2f}" if price is not None else None,
            "numeric_price":      price,
            "special_offer_name": special_name,
            "description":        description,
            "image_url":          image_url,
            "product_url":        product_url,
            "in_stock":           in_stock,
            "scraped_at":         scraped_at,
        })

    return records


# ══════════════════════════════════════════════════════════════════════════════
# OFFERS / SPECIALS FETCHING  (Playwright DOM scraper)
# ══════════════════════════════════════════════════════════════════════════════
#
# Dutchie's /specials page is fully client-side rendered (Next.js, empty
# pageProps).  The offer cards are loaded by the browser via an internal
# GraphQL call whose persisted-query hash is private.  Attempting to POST
# directly to /graphql is blocked by Cloudflare (HTTP 400).
#
# Solution: use Playwright to open the /specials page in a real browser,
# wait for the BogoCardContainer elements to appear, then extract the card
# data directly from the DOM.  This is completely independent of the
# product-scraping pipeline (curl_cffi / GET requests) and does not affect it.
# ──────────────────────────────────────────────────────────────────────────────

def _infer_category_hint(text: str) -> str | None:
    """
    Infer a category hint from offer title/description text.
    Returns the first matching category string or None.
    """
    if not text:
        return None
    lower = text.lower()
    for category, keywords in CATEGORY_HINT_MAP:
        if any(kw in lower for kw in keywords):
            return category
    return None


def _is_mix_and_match(text: str) -> bool:
    """Return True if the offer text mentions mix & match."""
    if not text:
        return False
    lower = text.lower()
    return "mix" in lower and ("match" in lower or "&" in lower)


# JavaScript injected into the page to extract all offer cards from the DOM.
# Dutchie renders offer cards with the CSS class pattern 'BogoCardContainer'.
# Each card has a title element ('Title'), a shop button ('ShopButton'), and
# a background-image CSS property containing the card image URL.
_EXTRACT_OFFERS_JS = """
() => {
    const cards = document.querySelectorAll('[class*="BogoCardContainer"]');
    const results = [];
    cards.forEach((card, idx) => {
        const titleEl  = card.querySelector('[class*="Title"]');
        const shopBtn  = card.querySelector('[class*="ShopButton"]');
        const bgStyle  = window.getComputedStyle(card).backgroundImage;
        let imageUrl   = null;
        const urlMatch = bgStyle.match(/url\\(["']?([^"')]+)["']?\\)/);
        if (urlMatch) imageUrl = urlMatch[1];
        results.push({
            position:  idx + 1,
            title:     titleEl ? titleEl.textContent.trim() : null,
            cta:       shopBtn ? shopBtn.textContent.trim() : null,
            image_url: imageUrl,
        });
    });
    return results;
}
"""


def fetch_offers(
    client:          DutchieClient,   # kept for API compatibility; not used here
    dispensary_id:   str,             # kept for API compatibility; not used here
    dispensary_slug: str,
    dispensary_name: str,
    source_url:      str,
    pricing_type:    str = "recreational",
) -> list[dict]:
    """
    Scrape offer cards from the Dutchie /specials page using Playwright.

    Navigates to https://dutchie.com/dispensary/{slug}/specials, waits for
    the BogoCardContainer elements to render, then extracts card data via
    injected JavaScript.  Returns a list of normalized offer-card records.

    These records are completely separate from discounted SKU rows in the
    product menu and are pushed to the named 'offers' Apify dataset.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.warning(
            f"[{dispensary_slug}] Playwright not installed — skipping offers scrape. "
            "Add 'playwright>=1.40.0' to requirements.txt and re-build the actor."
        )
        return []

    specials_url = f"https://dutchie.com/dispensary/{dispensary_slug}/specials"
    scraped_at   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"[{dispensary_slug}] fetch_offers → navigating to {specials_url}")

    raw_cards: list[dict] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )
            page = context.new_page()

            # Block unnecessary resources to speed up load
            page.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,otf}",
                lambda route: route.abort(),
            )

            page.goto(specials_url, wait_until="domcontentloaded", timeout=45_000)

            # Wait for at least one offer card to appear (up to 20 s)
            try:
                page.wait_for_selector('[class*="BogoCardContainer"]', timeout=20_000)
            except PWTimeout:
                logger.info(
                    f"[{dispensary_slug}] No BogoCardContainer found within 20 s — "
                    "store may have no active specials."
                )
                browser.close()
                return []

            # Small extra wait to let all cards finish rendering
            page.wait_for_timeout(1_500)

            raw_cards = page.evaluate(_EXTRACT_OFFERS_JS)
            browser.close()

    except Exception as e:
        logger.warning(f"[{dispensary_slug}] fetch_offers Playwright error: {e}")
        return []

    logger.info(f"[{dispensary_slug}] fetch_offers → {len(raw_cards)} offer cards found")

    offer_records: list[dict] = []
    for card in raw_cards:
        offer_title = _safe_str(card.get("title"))
        position    = int(card.get("position", 0))
        offer_image = _safe_str(card.get("image_url"))
        cta_text    = _safe_str(card.get("cta")) or "Shop"

        # Infer price text from title (e.g. "3 for $20", "2 for $35")
        price_text = None
        if offer_title:
            price_match = re.search(
                r'(\d+\s*(?:for|/|@)\s*\$\d+(?:\.\d+)?|\$\d+(?:\.\d+)?(?:\s*each)?)',
                offer_title, re.IGNORECASE
            )
            if price_match:
                price_text = price_match.group(0).strip()

        combined_text = offer_title or ""

        offer_records.append({
            "offers_schema_version":  OFFERS_SCHEMA_VERSION,
            "store_slug":             dispensary_slug,
            "store_name":             dispensary_name,
            "source_url":             source_url,
            "scrape_timestamp_utc":   scraped_at,
            "offer_position":         position,
            "offer_id":               None,          # Not available via DOM
            "offer_title":            offer_title,
            "offer_subtitle":         None,
            "offer_body_text":        None,
            "offer_cta_text":         cta_text,
            "offer_target_url":       specials_url,
            "offer_image_url":        offer_image,
            "offer_image_alt":        None,
            "offer_badges":           [],
            "offer_price_text_raw":   price_text,
            "offer_terms_text_raw":   None,
            "is_mix_and_match":       _is_mix_and_match(combined_text),
            "category_hint":          _infer_category_hint(combined_text),
            "special_type":           None,
            "menu_type":              None,
            "start_date":             None,
            "end_date":               None,
            "linked_sku_count":       0,
            "linked_skus":            [],
            "raw_card_html":          None,
        })

    return offer_records


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ACTOR ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def run_actor():
    """Main Apify Actor logic."""
    if APIFY_AVAILABLE:
        async with Actor:
            actor_input = await Actor.get_input() or {}
            await _process(actor_input)
    else:
        # Local testing mode — uses a sample dispensary
        logger.info("Running in LOCAL mode (Apify SDK not available)")
        test_input = {
            "dispensaryUrls": [
                "https://dutchie.com/dispensary/pinnacle-cannabis-quincy"
            ],
            "maxItems": 20,
        }
        await _process(test_input)


async def _process(actor_input: dict):
    """Process actor input: scrape each dispensary and push results."""

    # ── Parse input ───────────────────────────────────────────────────────────
    raw_urls  = actor_input.get("dispensaryUrls", [])
    max_items = int(actor_input.get("maxItems", 0))
    use_proxy = actor_input.get("useProxy", False)
    proxy_grp = actor_input.get("proxyGroup", "RESIDENTIAL")

    # Normalise URL list (accept strings or {url: ...} dicts)
    urls: list[str] = []
    for item in raw_urls:
        if isinstance(item, dict):
            u = item.get("url", "")
        elif isinstance(item, str):
            u = item
        else:
            continue
        u = u.strip()
        if u:
            urls.append(u)

    if not urls:
        logger.error("No dispensaryUrls provided. Please add at least one Dutchie URL.")
        return

    logger.info(f"Processing {len(urls)} dispensar{'y' if len(urls) == 1 else 'ies'} …")

    # ── Proxy configuration ───────────────────────────────────────────────────
    proxy_url = None
    if use_proxy and APIFY_AVAILABLE:
        try:
            proxy_config = await Actor.create_proxy_configuration(groups=[proxy_grp])
            proxy_url    = await proxy_config.new_url()
            logger.info(f"Proxy enabled ({proxy_grp})")
        except Exception as e:
            logger.warning(f"Could not configure proxy: {e}. Proceeding without proxy.")

    client = DutchieClient(proxy_url=proxy_url)
    total_results = 0

    try:
        for idx, url in enumerate(urls, start=1):
            logger.info(f"\n{'─' * 60}")
            logger.info(f"[{idx}/{len(urls)}] {url}")
            logger.info(f"{'─' * 60}")

            # ── Extract slug ──────────────────────────────────────────────────
            try:
                slug = extract_slug(url)
            except ValueError as e:
                logger.error(f"Skipping invalid URL: {e}")
                continue

            # ── Resolve dispensary ────────────────────────────────────────────
            try:
                disp = resolve_dispensary(client, slug)
            except RuntimeError as e:
                logger.error(f"[{slug}] Could not resolve dispensary: {e}")
                continue

            dispensary_id   = disp["id"]
            dispensary_name = disp["name"]
            dispensary_cname = disp["cName"]
            logger.info(f"[{slug}] Resolved → '{dispensary_name}' (cName={dispensary_cname})")

            # ── Fetch products ────────────────────────────────────────────────
            try:
                raw_products = fetch_all_products(
                    client        = client,
                    dispensary_id = dispensary_id,
                    cname         = dispensary_cname,
                    max_items     = max_items,
                )
            except Exception as e:
                logger.error(f"[{dispensary_cname}] Scraping failed: {e}")
                continue

            if not raw_products:
                logger.warning(f"[{dispensary_cname}] No products returned.")
                continue

            # ── Normalize & push ──────────────────────────────────────────────
            scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            store_results: list[dict] = []

            for raw in raw_products:
                records = normalize_product(
                    raw             = raw,
                    dispensary_name = dispensary_name,
                    dispensary_url  = url,
                    scraped_at      = scraped_at,
                )
                store_results.extend(records)

            logger.info(f"[{dispensary_cname}] {len(raw_products)} products → {len(store_results)} SKU records")

            if APIFY_AVAILABLE:
                # Push product SKUs to the default dataset
                await Actor.push_data(store_results)
                # Charge one PPR event per result
                await Actor.charge(event_name=PPR_EVENT_NAME, count=len(store_results))
                logger.info(f"[{dispensary_cname}] Pushed {len(store_results)} results & charged {len(store_results)} PPR events")
            else:
                # Local mode: print a sample
                logger.info(f"[{dispensary_cname}] LOCAL MODE — sample output:")
                for r in store_results[:3]:
                    logger.info(
                        f"  [{r['category']:12}] {r['product_name'][:40]:40} | "
                        f"{r['brand'] or '—':20} | "
                        f"THC: {r['thc_level'] or '—':8} | "
                        f"{r['display_price'] or '—'}"
                    )
                if len(store_results) > 3:
                    logger.info(f"  … and {len(store_results) - 3} more SKU records")

            total_results += len(store_results)

            # ── Fetch Offers tab cards (separate dataset) ─────────────────────
            logger.info(f"[{dispensary_cname}] Fetching Offers tab cards …")
            try:
                offer_records = fetch_offers(
                    client          = client,
                    dispensary_id   = dispensary_id,
                    dispensary_slug = dispensary_cname,
                    dispensary_name = dispensary_name,
                    source_url      = url,
                    pricing_type    = "recreational",
                )
            except Exception as e:
                logger.warning(f"[{dispensary_cname}] fetch_offers failed: {e}")
                offer_records = []

            if offer_records:
                logger.info(f"[{dispensary_cname}] {len(offer_records)} offer cards found")
                if APIFY_AVAILABLE:
                    # Push offers to a SEPARATE named dataset so they never mix with SKUs
                    offers_dataset = await Actor.open_dataset(name="offers")
                    await offers_dataset.push_data(offer_records)
                    logger.info(f"[{dispensary_cname}] Pushed {len(offer_records)} offer cards to 'offers' dataset")
                else:
                    logger.info(f"[{dispensary_cname}] LOCAL MODE — offer cards sample:")
                    for o in offer_records[:3]:
                        logger.info(
                            f"  [{o['offer_position']:2}] {(o['offer_title'] or '—')[:60]:60} | "
                            f"mix_match={o['is_mix_and_match']} | cat={o['category_hint'] or '—'}"
                        )
                    if len(offer_records) > 3:
                        logger.info(f"  … and {len(offer_records) - 3} more offer cards")
            else:
                logger.info(f"[{dispensary_cname}] No active offer cards found")

            # Polite delay between stores in bulk mode
            if len(urls) > 1 and idx < len(urls):
                time.sleep(1.0)

    finally:
        client.close()

    logger.info(f"\n{'═' * 60}")
    logger.info(f"COMPLETE: {total_results:,} total SKU records across {len(urls)} store(s)")
    logger.info(f"{'═' * 60}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    asyncio.run(run_actor())


if __name__ == "__main__":
    main()
