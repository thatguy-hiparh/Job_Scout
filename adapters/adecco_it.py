# adapters/adecco_it.py
import re, time
from datetime import datetime, timedelta
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; job_scout/1.0; +https://example.org)",
    "Accept-Language": "it-IT,it;q=0.8,en-US;q=0.6,en;q=0.4",
}

RELATIVE_RE = re.compile(r"(\d+)\s+(giorni|settimane|mesi)\s+fa", re.I)

def _parse_date_it(text):
    if not text:
        return None
    t = text.strip().lower()
    if "oggi" in t:
        return datetime.utcnow().date().isoformat()
    if "ieri" in t:
        return (datetime.utcnow() - timedelta(days=1)).date().isoformat()
    m = RELATIVE_RE.search(t)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        if unit.startswith("giorn"):
            delta = timedelta(days=n)
        elif unit.startswith("settim"):
            delta = timedelta(weeks=n)
        elif unit.startswith("mes"):
            delta = timedelta(days=30 * n)
        else:
            delta = timedelta(days=n)
        return (datetime.utcnow() - delta).date().isoformat()
    # Accept ISO-like dates if present
    m2 = re.search(r"(\d{4})-(\d{2})-(\d{2})", t)
    if m2:
        y, mo, d = map(int, m2.groups())
        try:
            return datetime(y, mo, d).date().isoformat()
        except Exception:
            return None
    return None

def _clean(s):
    return re.sub(r"\s+", " ", s).strip() if s else s

def fetch_adecco_it(base="https://www.adecco.it/offerte-lavoro", query=None, max_pages=10, pause=0.8, debug=False):
    """
    Scrapes Adecco Italy job listings (public site).
    Pagination pattern observed:
      - page 1:  base URL exactly
      - page N:  ?page=N
    Optional free-text 'k' query may or may not be honored; we append conservatively.
    """
    items = []
    s = requests.Session()
    s.headers.update(HEADERS)

    for page in range(1, max_pages + 1):
        # Build URL safely for their routing (avoid l= / p= which 404'ed)
        if page == 1:
            url = base
        else:
            url = f"{base}?page={page}"
        if query:
            # Append ?k= only if present; avoid breaking page param
            joiner = "&" if "?" in url else "?"
            url = f"{url}{joiner}k={query}"

        r = s.get(url, timeout=20)
        if debug:
            print(f"ADECCO_IT GET {url} -> {r.status_code}")
        if r.status_code != 200:
            # stop on 404/other errors
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # Try multiple card containers
        cards = (
            soup.select("article") or
            soup.select("div.job-card") or
            soup.select("li.job-result")
        )
        if debug:
            print(f"ADECCO_IT page {page}: found cards={len(cards)}")
        if not cards:
            if page == 1:
                # site might lazy-load via JS; try alternative markers
                cards = soup.select("[data-job-id], .search-result, .job-listing")
            if not cards:
                break

        kept = 0
        for c in cards:
            # Title
            te = (
                c.select_one("a h3") or
                c.select_one("h3 a") or
                c.select_one("a[data-testid='job-title']") or
                c.select_one("h2 a") or
                c.select_one("a.job-title")
            )
            title = _clean(te.get_text()) if te else None

            # URL
            link = None
            a = c.select_one("a[href]")
            if a and a.get("href"):
                href = a["href"]
                link = href if href.startswith("http") else urljoin(base, href)

            # Company
            ce = c.select_one(".company, [data-testid='company-name'], .job-company")
            company = _clean(ce.get_text()) if ce else "Adecco"

            # Location
            le = c.select_one(".location, [data-testid='job-location'], .job-location, .job-city")
            loc = _clean(le.get_text()) if le else "Italia"

            # Posted date
            de = c.select_one("time, .date, [data-testid='posted-date'], .job-date")
            date_text = _clean(de.get_text()) if de else None
            posted = _parse_date_it(date_text)

            if not title or not link:
                continue

            items.append({
                "title": title,
                "company": company or "Adecco",
                "location": loc,
                "posted_at": posted,
                "url": link,
                "raw": {
                    "date_text": date_text,
                },
            })
            kept += 1

        if debug:
            print(f"ADECCO_IT page {page}: kept {kept}")
        if kept == 0:
            break
        time.sleep(pause)

    return items

def run(source_cfg, keyword_pass, location_pass, recency_pass, debug=False):
    base = source_cfg.get("base_url", "https://www.adecco.it/offerte-lavoro")
    query = source_cfg.get("query")
    max_pages = int(source_cfg.get("max_pages", 10))
    pause = float(source_cfg.get("pause_s", 0.8))

    raw = fetch_adecco_it(base=base, query=query, max_pages=max_pages, pause=pause, debug=debug)
    if debug:
        print(f"ADECCO_IT RAW {len(raw)}")

    pre = len(raw)
    stage_kw = [x for x in raw if keyword_pass(x, source_cfg)]
    stage_loc = [x for x in stage_kw if location_pass(x, source_cfg)]
    stage_dt = [x for x in stage_loc if recency_pass(x, source_cfg)]

    if debug:
        print(f"SMART_FILTER Adecco_IT: pre={pre} kw={len(stage_kw)} loc={len(stage_loc)} date={len(stage_dt)}")

    return stage_dt

def fetch(cfg):
    base = cfg.get("base_url", "https://www.adecco.it/offerte-lavoro")
    query = cfg.get("query")
    max_pages = int(cfg.get("max_pages", 10))
    pause = float(cfg.get("pause_s", 0.8))
    debug = bool(cfg.get("debug", False))
    return fetch_adecco_it(base=base, query=query, max_pages=max_pages, pause=pause, debug=debug)
