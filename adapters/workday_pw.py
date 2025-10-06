# adapters/workday_pw.py
import os
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# Env knobs
PW_HEADLESS       = os.getenv("PW_HEADLESS", "1") == "1"
WD_MAX_PAGES      = int(os.getenv("WD_MAX_PAGES", "3"))
WD_LIMIT          = int(os.getenv("WD_LIMIT", "200"))
WD_EARLY_BREAK    = os.getenv("WD_EARLY_BREAK", "1") == "1"
WORKDAY_PW_DEBUG  = os.getenv("WORKDAY_PW_DEBUG", "0") == "1"

# Public UI selectors (not the /wday/cxs/ JSON app shell)
SEL_JOB_TILE      = '[data-automation-id="jobTile"]'
SEL_JOB_TITLE     = 'a[data-automation-id="jobTitle"]'
SEL_LOCATION      = '[data-automation-id="locations"]'
SEL_POSTED        = '[data-automation-id="postedOn"]'
SEL_NEXT_PAGE_BTN = 'button[aria-label="Next Page"]'

# Fallback selectors seen on some tenants
FALLBACK_TITLE_LINKS = 'a[data-automation-id="jobTitle"], a[role="link"][data-automation-id="jobTitle"]'
FALLBACK_TILE        = '[data-automation-id="jobResults"] [data-automation-id="jobTile"], [data-automation-id="jobPosting"]'

CONSENT_BUTTONS = [
    'button:has-text("Accept")',
    'button:has-text("I Accept")',
    'button:has-text("Allow all")',
    'button:has-text("Agree")',
    'button:has-text("Accept all")',
]

def _d(msg):
    if WORKDAY_PW_DEBUG:
        print(msg)

def _safe_text(loc):
    try:
        return " ".join(loc.inner_text().split())
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

def _wait_for_jobs(page):
    """Wait a bit for either canonical or fallback job elements."""
    try:
        page.wait_for_selector(SEL_JOB_TILE, timeout=12000)
        return True
    except PWTimeout:
        # Nudge lazy-loading
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        try:
            page.wait_for_selector(FALLBACK_TILE, timeout=8000)
            return True
        except PWTimeout:
            return False

def _scrape_current_page(page, base_url, company, sink):
    found = 0
    tiles = page.locator(SEL_JOB_TILE)
    count = tiles.count()
    if count == 0:
        # Fallback: walk title links if we didn't detect tiles
        links = page.locator(FALLBACK_TITLE_LINKS)
        for i in range(min(links.count(), max(0, WD_LIMIT - len(sink)))):
            a = links.nth(i)
            href = a.get_attribute("href") or ""
            title = _safe_text(a)
            url = urljoin(base_url, href)
            if not title or not href:
                continue
            sink.append({
                "title": title,
                "company": company,
                "location": "",
                "posted": "",
                "url": url,
                "source": "workday",
            })
            found += 1
        return found

    for i in range(count):
        if len(sink) >= WD_LIMIT:
            break
        tile = tiles.nth(i)
        title_el = tile.locator(SEL_JOB_TITLE).first
        if not title_el or title_el.count() == 0:
            continue
        title = _safe_text(title_el)
        href = title_el.get_attribute("href") or ""
        url = urljoin(base_url, href)
        loc = _safe_text(tile.locator(SEL_LOCATION).first)
        posted = _safe_text(tile.locator(SEL_POSTED).first)
        if title and href:
            sink.append({
                "title": title,
                "company": company,
                "location": loc,
                "posted": posted,
                "url": url,
                "source": "workday",
            })
            found += 1
    return found

def _open(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=45000)
    # clear cookie banners if any
    for sel in CONSENT_BUTTONS:
        _click_if_present(page, sel)

def fetch(company_cfg):
    """
    company_cfg:
      - name
      - ats: 'workday_pw'
      - workday_hosts: ['umusic.wd5.myworkdayjobs.com', ...]
      - workday_sites: ['UMGUS', 'UMGUK', ...]
      - optional: workday_tenant (overrides tenant derivation)
    """
    name  = company_cfg.get("name", "Unknown")
    hosts = company_cfg.get("workday_hosts") or []
    sites = company_cfg.get("workday_sites") or []
    tenant = company_cfg.get("workday_tenant")

    tried = []
    out = []

    # Candidate public UI URL patterns per host/site
    def url_candidates(host, site, tnt):
        # Workday public app usually lives at /{site} (locale often implicit)
        base1 = f"https://{host}/{site}"
        base2 = f"https://{host}/{site}/"  # trailing slash variant
        # some tenants expose /{site}/careers or /{site}/jobs
        base3 = f"https://{host}/{site}/careers"
        base4 = f"https://{host}/{site}/jobs"
        # absolute fallback to the cxs app shell (rarely renders full UI, but try)
        base5 = f"https://{host}/wday/cxs/{tnt}/{site}"
        return [base1, base2, base3, base4, base5]

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=PW_HEADLESS)
        ctx = browser.new_context()
        page = ctx.new_page()

        for host in hosts:
            tnt = tenant or (host.split(".", 1)[0] if "." in host else "")
            for site in sites:
                if len(out) >= WD_LIMIT:
                    break
                items_before = len(out)
                status = "ok"
                got_items = 0

                for candidate in url_candidates(host, site, tnt):
                    try:
                        _open(page, candidate)
                        if not _wait_for_jobs(page):
                            continue

                        pages = 0
                        while pages < WD_MAX_PAGES and len(out) < WD_LIMIT:
                            added = _scrape_current_page(page, candidate, name, out)
                            got_items += added
                            pages += 1
                            if added == 0 and WD_EARLY_BREAK:
                                break
                            # paginate if possible
                            next_btn = page.locator(SEL_NEXT_PAGE_BTN)
                            if next_btn.count() and next_btn.first.is_enabled():
                                try:
                                    next_btn.first.click(timeout=2000)
                                    page.wait_for_timeout(600)
                                    if not _wait_for_jobs(page):
                                        break
                                except Exception:
                                    break
                            else:
                                break
                        # If we got anything from this candidate, stop trying other variants for this site
                        if got_items > 0:
                            break
                    except Exception as e:
                        status = f"error:{type(e).__name__}"

                tried.append({"host": host, "site": site, "url": "…", "status": status, "items": got_items})

        ctx.close()
        browser.close()

    if WORKDAY_PW_DEBUG:
        print(f"WORKDAY_PW_DEBUG {name}: tried={tried} got={len(out)}")

    return out
