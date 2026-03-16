# Dutchie Dispensary Menu Scraper

[![Apify Actor](https://img.shields.io/badge/Apify-Actor-green)](https://apify.com/)
[![Version](https://img.shields.io/badge/version-1.0.1-blue)](https://github.com/tfmcg3/dutchie-dispensary-scraper)
[![Python](https://img.shields.io/badge/python-3.11-yellow)](https://python.org/)

Extract structured product menus from public Dutchie-powered dispensary pages in about 30–60 seconds per store, with low compute cost and per-variant SKU output. Output is normalized for direct use in spreadsheets, BI dashboards, diff monitoring, and price-change alerts.

---

## 🚀 Quick Start

### Single Dispensary (Default — ~30 seconds)

```json
{
  "dispensaryUrls": ["https://dutchie.com/dispensary/your-store-name"],
  "maxItems": 0
}
```

### Bulk / Competitor Monitoring

```json
{
  "dispensaryUrls": [
    "https://dutchie.com/dispensary/store-one",
    "https://dutchie.com/dispensary/store-two",
    "https://dutchie.com/dispensary/store-three"
  ],
  "maxItems": 0,
  "useProxy": true
}
```

### Quick Preview (10 products)

```json
{
  "dispensaryUrls": ["https://dutchie.com/dispensary/your-store-name"],
  "maxItems": 10
}
```

---

## ✨ Features

- **API-based extraction** — no browser, no Puppeteer, no Playwright; uses Dutchie's own GraphQL API for speed and efficiency.
- **Improved access reliability** — uses Chrome-like TLS fingerprinting via `curl_cffi`; residential proxies are supported for harder targets and bulk runs.
- **Automatic dispensary resolution** — automatically resolves the dispensary’s internal Dutchie identifier from the supplied URL using multiple fallback methods.
- **Full menu pagination** — fetches every page until the menu is complete (up to 10,000 products).
- **Per-variant output** — each size/weight option is its own record for accurate pricing analysis.
- **Special offers** — captures active deal names and promotions.
- **Bulk mode** — scrape multiple dispensaries in a single run.
- **Proxy support** — optional Apify residential/datacenter proxy for high-volume use.

---

## ✅ Best For / ❌ Not For

| Best For | Not Ideal For |
|---|---|
| Public recreational menus | Login-protected menus |
| Daily competitor monitoring | Medical-only menus requiring patient access |
| Pricing and assortment analysis | Full visual page rendering or screenshot capture |
| Bulk regional scans | Historical data unless you schedule recurring runs |

---

## 📊 Input Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dispensaryUrls` | `string[]` | **required** | One or more Dutchie dispensary URLs |
| `maxItems` | `integer` | `0` (unlimited) | Max products per store; `0` = full menu |
| `useProxy` | `boolean` | `false` | Enable Apify proxy rotation |
| `proxyGroup` | `string` | `RESIDENTIAL` | `RESIDENTIAL` or `DATACENTER` |

---

## 📤 Output Schema

All schema fields are included whenever available; unavailable values are returned as `null`.

```json
{
  "schema_version":    "1.0",
  "dispensary_name":   "Pinnacle Cannabis",
  "dispensary_url":    "https://dutchie.com/dispensary/pinnacle-cannabis-quincy",
  "product_id":        "62d9c8e4a9e3d400b1e3d2a1",
  "product_name":      "1906 Drops - Genius",
  "brand":             "1906",
  "category":          "Edible",
  "subcategory":       "Pill",
  "strain_type":       null,
  "thc_level":         "2.5mg",
  "cbd_level":         "2.5mg",
  "variant_size":      "20 Pack",
  "display_price":     "$25.00",
  "numeric_price":     25.00,
  "special_offer_name": "2 for $40 Mix & Match Edibles",
  "description":       "A microdose blend of THC and CBD for focus and creativity.",
  "image_url":         "https://images.dutchie.com/...",
  "product_url":       "https://dutchie.com/dispensary/pinnacle-cannabis-quincy/product/62d9c8e4a9e3d400b1e3d2a1",
  "in_stock":          true,
  "scraped_at":        "2026-03-15T10:30:00Z"
}
```

---

## 💰 Pricing

**Pay-Per-Result: $1.50 per 1,000 product variants (SKUs)**

Most single-store runs cost under $2. **You are only billed for results successfully written to the dataset** — not for attempted scrapes, failed requests, or empty runs. Test with `maxItems: 10` for a low-cost validation run before committing to a full pull.

| Scenario | Approx. Products | Estimated Cost |
|---|---|---|
| Single small store | ~200 SKUs | ~$0.30 |
| Single large store | ~1,000 SKUs | ~$1.50 |
| 5 stores (bulk) | ~5,000 SKUs | ~$7.50 |
| 10 stores (bulk) | ~10,000 SKUs | ~$15.00 |
| High-frequency (1 store, 1000 SKUs, every 15 min) | ~96,000 SKUs/day | ~$144/day |

**Note on high-frequency use:** This Actor is designed for batch runs — daily or hourly full menu pulls. High-frequency polling (e.g., every 5 minutes on a 1,000-SKU store) would cost approximately **$432/day for a single store**, which is not a cost-effective use of this tool. For real-time or sub-hourly monitoring, a lightweight custom Actor with a flat-rate pricing model is the better fit.

---

## 🌐 Supported Dispensaries

Supports many public Dutchie-powered dispensary menus in the U.S. and Canada. Performance may vary based on menu configuration, location gating, and anti-bot protections.

**Supported URL formats:**
- `https://dutchie.com/dispensary/store-name`
- `https://dutchie.com/dispensary/store-name/menu`
- `https://dutchie.com/embedded-menu/store-name`

---

## 🛡️ Tested & Verified

This Actor has been tested against:
- Single-store runs on public rec menus
- Bulk runs across multiple competitor menus
- Menus with multiple categories and variants
- Residential proxy runs for harder stores
- Invalid URLs and non-existent dispensaries

---

## ⚠️ Known Limitations

- **Menu Type:** Scrapes public recreational (rec) menus by default. Medical-only or dual-license menus may return different or partial data.
- **Proxy Use:** Some stores may require residential proxies to avoid blocks.
- **Gated Content:** Age-gated or region-gated menus may return partial or no data.
- **Upstream Changes:** This Actor relies on Dutchie’s public-facing GraphQL menu infrastructure. Upstream API changes may require maintenance updates to the Actor.
- **Retry Behavior:** On failed requests, the Actor retries up to 3 times with exponential backoff. If a store returns partial data, the Actor logs a warning but completes the run with the data it has.

---

## 🛠️ Troubleshooting

**"Could not resolve dispensary" error**
- Open the dispensary URL in your browser and copy the exact URL from the address bar
- The slug in the URL after `/dispensary/` is the correct identifier

**403 Forbidden / Cloudflare block**
- Enable `useProxy: true` and set `proxyGroup: "RESIDENTIAL"`

**Fewer products than expected**
- Some stores have separate recreational and medical menus; the actor scrapes the recreational (rec) menu by default
- Verify the store's menu is publicly accessible without login

**Actor times out**
- Increase the Actor timeout in your Apify console settings (default is 60s; set to 300s for large stores)
- Use `maxItems` to limit output during testing

---

## 📄 Compliance Notice

This Actor is provided for lawful data extraction purposes only. It uses Dutchie's public-facing GraphQL API, which serves their own frontend, and respects rate limits to avoid disrupting service. Users are solely responsible for ensuring their use of this Actor complies with Dutchie's [Terms of Service](https://dutchie.com/terms-of-service), all applicable local, state, and federal laws, and the terms of service of any dispensary whose data is extracted.

---

**Questions or issues?** Use the Issues tab on this Actor's page or contact via Apify Actor support.
