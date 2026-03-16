# Apify Store Publication Guide — Dutchie Dispensary Scraper

## Pre-Publication Checklist

All items below must be completed before submitting for Apify Store review.

### Code & Configuration
- [x] `src/main.py` — production-ready, no hardcoded credentials, no company-specific references
- [x] `.actor/actor.json` — actor metadata, title, description, dataset views
- [x] `.actor/input_schema.json` — user-friendly input form with descriptions and prefill values
- [x] `.actor/charges.json` — PPR billing at $1.50/1,000 results
- [x] `Dockerfile` — uses `apify/actor-python:3.11` base image
- [x] `requirements.txt` — `apify>=2.0.0`, `curl_cffi>=0.7.0`
- [x] `README.md` — store-grade documentation with Quick Start, schema, pricing, troubleshooting
- [x] `.gitignore` — excludes `__pycache__`, `.env`, `storage/`, etc.

### Validation
- [x] 5/5 test URLs passing with zero schema errors
- [x] THC/CBD potency extraction working (98–100% coverage on MA stores)
- [x] Multi-variant products correctly split into individual SKU records
- [x] Prices, brands, categories all normalizing correctly
- [x] Cloudflare bypass working via `curl_cffi` Chrome fingerprint

### Sanitization (Public Safety)
- [x] No references to Quincy Cannabis Co. or QCC
- [x] No hardcoded API keys or tokens
- [x] No internal company data in lookup table
- [x] README uses generic example dispensary URLs only

---

## Step-by-Step: Publishing to Apify Store

### Step 1: Create Actor on Apify Console

1. Go to https://console.apify.com/actors
2. Click **Create new Actor**
3. Choose **"Connect a Git repository"**
4. Connect to: `https://github.com/tfmcg3/dutchie-dispensary-scraper`
5. Set branch to `master`
6. Apify will auto-detect the `.actor/` folder and configure everything

### Step 2: Configure Monetization

1. In your Actor settings, go to **Monetization**
2. Select **Pay Per Result (PPR)**
3. The `charges.json` file already defines the pricing:
   - Event name: `product-scraped`
   - Price: **$1.50 per 1,000 results**
4. Save and confirm

### Step 3: Build & Test

1. Click **Build** to trigger the first build
2. Once built, click **Run** with the default input (Pinnacle Cannabis test URL)
3. Verify output in the **Dataset** tab — should show clean product records
4. Check the **Charges** tab to confirm PPR events are firing

### Step 4: Submit for Store Review

1. Go to **Actor Settings → Publication**
2. Fill in:
   - **Title:** `Dutchie Dispensary Menu Scraper`
   - **Description:** `Extract complete product menus from any Dutchie-powered dispensary in under 60 seconds.`
   - **Categories:** `E-commerce`, `Automation`
   - **Icon:** Upload a cannabis leaf or dispensary icon
3. The README.md content will automatically populate the Store page
4. Click **Submit for Review**

### Step 5: Post-Publication

- Monitor the **Issues** tab for user-reported problems
- Watch the **Usage** tab for first paying customers
- Consider adding more dispensary slugs to `dispensary_lookup.json` over time

---

## Pricing Strategy

| Competitor | Price | Notes |
|---|---|---|
| Leafly Scraper (paradox-analytics) | $3.00/1,000 | Direct competitor |
| Our Actor | **$1.50/1,000** | 50% cheaper — competitive advantage |

At $1.50/1,000 results:
- A user scraping 10 stores × 500 products = 5,000 SKUs = **$7.50 per run**
- A user running daily for a month = ~$225/month per customer
- At 10 paying customers = ~$2,250/month passive income

---

## GitHub Repository

**URL:** https://github.com/tfmcg3/dutchie-dispensary-scraper

This repo is currently **private**. Make it **public** before submitting to the Apify Store
(Apify requires public repos for Store actors, or you can use the Apify CLI to push directly).

To make public:
```bash
gh repo edit tfmcg3/dutchie-dispensary-scraper --visibility public
```
