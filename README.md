# Dutchie Dispensary Menu Scraper

[![Apify Actor](https://img.shields.io/badge/Apify-Actor-green)](https://apify.com/)
[![Version](https://img.shields.io/badge/version-1.0.0-blue)](https://github.com/)
[![Python](https://img.shields.io/badge/python-3.11-yellow)](https://python.org/)

Extract complete, structured product menus from any [Dutchie.com](https://dutchie.com)-powered dispensary in under 60 seconds. Returns clean data for every product variant (SKU): name, brand, category, THC/CBD levels, pricing, size, and stock status — with zero browser automation and near-zero compute cost.

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

- **API-based extraction** — no browser, no Puppeteer, no Playwright; uses Dutchie's own GraphQL API
- **Cloudflare bypass** — Chrome TLS fingerprinting via `curl_cffi`; no proxy required for most stores
- **Automatic dispensary resolution** — paste any Dutchie URL; the actor resolves the internal ID automatically via a 4-step chain (direct query → lookup table → HTML scrape → search API)
- **Full menu pagination** — fetches every page until the menu is complete (up to 10,000 products)
- **Per-variant output** — each size/weight option is its own record for accurate pricing analysis
- **Special offers** — captures active deal names and promotions
- **Bulk mode** — scrape multiple dispensaries in a single run
- **Proxy support** — optional Apify residential/datacenter proxy for high-volume use

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

Each result is one product variant (SKU). All fields are always present; missing values are `null`.

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

### Field Reference

| Field | Type | Description |
|---|---|---|
| `schema_version` | `string` | Output schema version (`"1.0"`) |
| `dispensary_name` | `string` | Human-readable dispensary name |
| `dispensary_url` | `string` | Input URL for this dispensary |
| `product_id` | `string` | Dutchie internal product ID |
| `product_name` | `string` | Full product name |
| `brand` | `string\|null` | Brand/manufacturer name |
| `category` | `string` | Normalized category (Flower, Vape, Edible, Pre-Roll, Concentrate, Tincture, Topical, Accessory, etc.) |
| `subcategory` | `string\|null` | Dutchie subcategory (e.g. Pill, Gummy, Live Resin) |
| `strain_type` | `string\|null` | Indica, Sativa, Hybrid, or CBD |
| `thc_level` | `string\|null` | THC potency (e.g. `"22.5%"`, `"10mg"`, `"18.0–24.0%"`) |
| `cbd_level` | `string\|null` | CBD potency |
| `variant_size` | `string\|null` | Size/weight of this variant (e.g. `"1g"`, `"3.5g"`, `"5 Pack"`) |
| `display_price` | `string\|null` | Formatted price (e.g. `"$35.00"`) |
| `numeric_price` | `number\|null` | Price as a float for sorting/analysis |
| `special_offer_name` | `string\|null` | Active deal or promotion name |
| `description` | `string\|null` | Product description |
| `image_url` | `string\|null` | Product image URL |
| `product_url` | `string\|null` | Direct link to the product on Dutchie |
| `in_stock` | `boolean` | Whether the product is currently available |
| `scraped_at` | `string` | ISO 8601 UTC timestamp of the scrape |

---

## 💰 Pricing

**Pay-Per-Result: $1.50 per 1,000 product variants (SKUs)**

One billable result = one unique product variant. A product with three size options (1g, 3.5g, 7g) counts as three results.

| Scenario | Approx. Products | Estimated Cost |
|---|---|---|
| Single small store | ~200 SKUs | ~$0.30 |
| Single large store | ~1,000 SKUs | ~$1.50 |
| 5 stores (bulk) | ~5,000 SKUs | ~$7.50 |
| 10 stores (bulk) | ~10,000 SKUs | ~$15.00 |

Platform compute costs are negligible — this actor uses direct API calls, not browser automation.

---

## 🎯 Use Cases

- **Competitive pricing analysis** — monitor competitor menus daily and track price changes
- **Market research** — analyze product mix, brand distribution, and category trends across a region
- **Inventory monitoring** — track stock availability and new product launches
- **Deal aggregation** — collect active promotions and special offers
- **Business intelligence** — feed clean data into Google Sheets, dashboards, or BI tools
- **Price optimization** — identify pricing gaps and opportunities vs. local competitors

---

## 🌐 Supported Dispensaries

Any dispensary that uses Dutchie as its e-commerce platform. Dutchie powers thousands of licensed dispensaries across the United States and Canada, including both recreational and medical markets.

**Supported URL formats:**
- `https://dutchie.com/dispensary/store-name`
- `https://dutchie.com/dispensary/store-name/menu`
- `https://dutchie.com/embedded-menu/store-name`

---

## 🔧 Technical Details

| Detail | Value |
|---|---|
| Extraction method | Dutchie GraphQL API (persisted queries) |
| Anti-bot bypass | Chrome TLS fingerprint via `curl_cffi` |
| Pagination | Page-based (50 products/page, up to 200 pages) |
| Retry logic | Exponential backoff, 3 retries per request |
| Rate limiting | 0.4s delay between pages |
| Max products | 10,000 per dispensary (safety cap) |
| Runtime | ~30s single store, ~1–2 min per store in bulk |

---

## 🛠️ Troubleshooting

**"Could not resolve dispensary" error**
- Open the dispensary URL in your browser and copy the exact URL from the address bar
- The slug in the URL after `/dispensary/` is the correct identifier
- Some stores use a different internal name than their display URL

**403 Forbidden / Cloudflare block**
- Enable `useProxy: true` and set `proxyGroup: "RESIDENTIAL"`
- This is rare but can occur for high-traffic stores

**Fewer products than expected**
- Some stores have separate recreational and medical menus; the actor scrapes the recreational (rec) menu by default
- Verify the store's menu is publicly accessible without login

**Actor times out**
- Increase the Actor timeout in your Apify console settings (default is 60s; set to 300s for large stores)
- Use `maxItems` to limit output during testing

---

## 📄 Compliance Notice

This Actor is provided for lawful data extraction purposes only. Users are solely responsible for ensuring their use of this Actor complies with:

- Dutchie's [Terms of Service](https://dutchie.com/terms-of-service)
- All applicable local, state, and federal laws
- The terms of service of any dispensary whose data is extracted

Do not use this Actor to collect personal data, circumvent access controls, or violate any applicable law.

---

## 🔄 Version History

### v1.0.0 (Current)
- Initial release
- API-based extraction with Chrome TLS fingerprint bypass
- 4-step automatic dispensary resolution chain
- Per-variant (SKU) output schema
- Pay-Per-Result monetization ($1.50 / 1,000 results)
- Single and bulk scraping modes
- Optional Apify proxy integration

---

**Questions or issues?** Use the Issues tab on this Actor's page or contact via Apify Actor support.
