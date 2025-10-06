# adapters/workday_pw.py
import os
import time
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# Read tuning knobs from env (safe defaults)
PW_HEADLESS = os.getenv("PW_HEADLESS", "1") == "1"
WD_MAX_PAGES = int(os.getenv("WD_MAX_PAGES", "3"))
WD_LIMIT     = int(os.getenv("WD_LIMIT", "200"))
WD_EARLY_BREAK = os.getenv("WD_EARLY_BREAK", "1") == "1"
WORKDAY_PW_DEBUG = os.getenv("WORKDAY_PW_DEBUG", "0") == "1"

# Common Workday selectors
SEL_JOB_TILE   = '[data-automation-id="jobTile"]'
SEL_JOB_TITLE  = 'a[data-automation-id="jobTitle"]'
SEL_LOCATION   = '[data-automation-id="locations"]'
SEL_POSTED     = '[data-automation-id="postedOn"]'
SEL_NEXT_PAGE  = 'button[aria-label="Next Page"]'

# A few likely cookie/consent buttons (click-if-present; ignore errors)
CONSENT_CANDIDATES = [
    'button:has-text("Accept")',
    'button:has-text("I Accept")',
    'button:has-text("Allow all")',
    'button:has-text("Agree")',
]

def _debug(msg):
    if WORKDAY_PW_DEBUG:
        print(msg)

def _safe_text(el):
    try:
        t = el.inner_text().strip()
        return " ".join(t.split())
    except Exception:
        return ""

def _click_if_present(page, selector):
    try:
        if page.locator(selector).first.is_visible():
            page.locator(selector).first.click(timeout=1000)
            return True
    except Exception:
        pass
    return False

def _scrape_listing_page(page, base_url, company, collected):
    """Scrape a single listing page into `collected` (list of job dicts)."""
    jobs_this_page = 0
    tiles = page.locator(SEL_JOB_TILE)
    count = tiles.count()
    for i in range(count):
        if len(collected) >= WD_LIMIT:
            break
        tile = tiles.nth(i)
        # Title + URL
        title_el = tile.locator(SEL_JOB_TITLE)
        if not title_el.count():
            continue
        title = _safe_text(title_el.first)
        href = title_el.first.get_attribute("href") or ""
        url = urljoin(base_url, href)

        # Location
        loc = _safe_text(tile.locator(SEL_LOCATION).first)

        # Posted date (if available)
        posted = _safe_text(tile.locator(SEL_POSTED).first)

        collected.append({
            "title": title,
            "company": company,
            "location": loc,
            "posted": posted,
            "url": url,
            "source": "workday",
        })
        jobs_this_page += 1
    return jobs_this_page

def _open_listing(page, url):
    """Open a listing page and wait for job tiles to appear (best-effort)."""
    page.goto(url, wait_until="domcontentloaded", timeout=45000)
    # Kill various popups that block scrolling / loading
    for sel in CONSENT_CANDIDATES:
        _click_if_present(page, sel)
    # Workday can lazy-load; give it a moment and try to detect tiles.
    try:
        page.wait_for_selector(SEL_JOB_TILE, timeout=12000)
    except PWTimeout:
        # Sometimes tiles render after a scroll
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_selector(SEL_JOB_TILE, timeout=8000)
        except PWTimeout:
            pass

def fetch(company_cfg):
    """
    company_cfg fields we rely on:
      - name
      - ats: 'workday_pw'
      - workday_hosts: ['umusic.wd5.myworkdayjobs.com', ...]
      - workday_sites: ['UMGUS', 'UMGUK', 'External', ...]
    """
    name = company_cfg.get("name", "Unknown")
    hosts = company_cfg.get("workday_hosts", []) or []
    sites = company_cfg.get("workday_sites", []) or []
    # Derive tenant (prefix of host, e.g., umusic.wd5.myworkdayjobs.com -> umusic)
    # If tenant is explicitly provided, prefer that:
    tenant = company_cfg.get("workday_tenant")

    results = []
    tried = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=PW_HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        for host in hosts:
            # derive tenant if needed
            tnt = tenant or (host.split(".", 1)[0] if "." in host else "")
            for site in sites:
                if len(results) >= WD_LIMIT:
                    break

                base = f"https://{host}/wday/cxs/{tnt}/{site}"
                status = "ok"
                items_before = len(results)
                try:
                    _open_listing(page, base)

                    # Page 1 + pagination
                    pages_done = 0
                    while pages_done < WD_MAX_PAGES and len(results) < WD_LIMIT:
                        got = _scrape_listing_page(page, base, name, results)
                        pages_done += 1
                        # Early-break if nothing found (helps on bad site keys)
                        if got == 0 and WD_EARLY_BREAK:
                            break
                        # Next page?
                        next_btn = page.locator(SEL_NEXT_PAGE)
                        if next_btn.count() and next_btn.first.is_enabled():
                            try:
                                next_btn.first.click(timeout=3000)
                                # wait a hair for new tiles to render
                                page.wait_for_timeout(800)
                                page.wait_for_selector(SEL_JOB_TILE, timeout=6000)
                            except Exception:
                                break
                        else:
                            break
                except Exception as e:
                    status = f"error:{type(e).__name__}"

                items_after = len(results)
                got_items = items_after - items_before
                tried.append({
                    "host": host,
                    "site": site,
                    "url": base,
                    "status": status,
                    "items": got_items
                })

        context.close()
        browser.close()

    if WORKDAY_PW_DEBUG:
        print(f"WORKDAY_PW_DEBUG {name}: tried={tried} got={len(results)}")

    return results
