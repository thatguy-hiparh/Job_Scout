# adapters/randstad_it.py
import re, time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlencode
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; job_scout/1.0; +https://example.org)",
    "Accept-Language": "it-IT,it;q=0.8,en-US;q=0.6,en;q=0.4",
}

# Italian date words to delta
RELATIVE_MAP = {
    r"oggi": 0,
    r"ieri": 1,
}
# e.g. "3 giorni fa", "2 settimane fa"
RELATIVE_RE = re.compile(r"(\d+)\s+(giorni|settimane|mesi)\s+fa", re.I)

def _parse_date_it(text):
    if not text:
        return None
    t = text.strip().lower()
    for kw, d in RELATIVE_MAP.items():
        if kw in t:
            return (datetime.utcnow() - timedelta(days=d)).date().isoformat()
    m = RELATIVE_RE.search(t)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        if unit.startswith("giorn"):
            delta = timedelta(days=n)
        elif unit.startswith("settim"):
            delta = timedelta(weeks=n)
        elif unit.startswith("mes"):
            # naive month ~30d
            delta = timedelta(days=30*n)
        else:
            delta = timedelta(days=n)
        return (datetime.utcnow() - delta).date().isoformat()

    # absolute like "03 settembre 2025" — best effort: strip month names
    months = {
        "gennaio": "01","febbraio":"02","marzo":"03","aprile":"04","maggio":"05","giugno":"06",
        "luglio":"07","agosto":"08","settembre":"09","ottobre":"10","novembre":"11","dicembre":"12"
    }
    m2 = re.search(r"(\d{1,2})\s+([a-zà]+)\s+(\d{4})", t)
    if m2:
        dd, mon_it, yyyy = m2.groups()
        mon = months.get(mon_it, "01")
        try:
            return datetime(int(yyyy), int(mon), int(dd)).date().isoformat()
        except Exception:
            pass
    # fallback: None (we’ll still keep the posting)
    return None

def _clean(s):
    return re.sub(r"\s+", " ", s).strip() if s else s

def fetch_randstad_it(base="https://www.randstad.it/offerte-lavoro/", query=None, max_pages=10, pause=0.8, debug=False):
    """
    Scrapes Randstad Italy search listing. We default to all Italy, paginate by ?pagina=N.
    Return list of dicts with title, company, location, posted_at, url, raw.
    """
    items = []
    session = requests.Session()
    session.headers.update(HEADERS)

    for page in range(1, max_pages+1):
        params = {}
        # Many Randstad URLs use path + ?pagina=
        params["pagina"] = page
        if query:
            params["testo"] = query  # if they support a free-text key (some deployments do)

        url = base
        if params:
            url = f"{base}?{urlencode(params)}"

        r = session.get(url, timeout=20)
        if debug: print(f"RANDSTAD_IT GET {url} -> {r.status_code}")
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # job cards: try several containers
        cards = soup.select("[data-component='job-card'], article, li.job-result, div.job-card")
        if debug: print(f"RANDSTAD_IT page {page}: found cards={len(cards)}")
        if not cards:
            # stop if first page empty; otherwise continue once in case of gap
            if page == 1:
                break
            else:
                continue

        page_count = 0
        for c in cards:
            # title
            title_el = (
                c.select_one("a h3") or
                c.select_one("h3 a") or
                c.select_one("a[data-testid='job-title']") or
                c.select_one("h2 a")
            )
            title = _clean(title_el.get_text()) if title_el else None

            # url
            link = None
            a = c.select_one("a[href]")
            if a and a.get("href"):
                href = a["href"]
                link = href if href.startswith("http") else urljoin(base, href)

            # company
            company = None
            ce = c.select_one("[data-testid='company-name'], .company, .job-company")
            company = _clean(ce.get_text()) if ce else "Randstad"

            # location
            loc = None
            le = c.select_one("[data-testid='job-location'], .location, .job-location, .job-city")
            loc = _clean(le.get_text()) if le else None

            # posted/date
            date_text = None
            de = c.select_one("[data-testid='posted-date'], time, .date, .job-date")
            date_text = _clean(de.get_text()) if de else None
            posted = _parse_date_it(date_text)

            # Skip if no title or no link
            if not title or not link:
                continue

            item = {
                "title": title,
                "company": company or "Randstad",
                "location": loc,
                "posted_at": posted,
                "url": link,
                "raw": {
                    "date_text": date_text,
                },
            }
            items.append(item)
            page_count += 1

        if debug: print(f"RANDSTAD_IT page {page}: kept {page_count}")
        # naive pagination stop: if page produced very few items, chances are we reached the end
        if page_count == 0:
            break
        time.sleep(pause)

    return items

def run(source_cfg, keyword_pass, location_pass, recency_pass, debug=False):
    base = source_cfg.get("base_url", "https://www.randstad.it/offerte-lavoro/")
    query = source_cfg.get("query")
    max_pages = int(source_cfg.get("max_pages", 10))
    pause = float(source_cfg.get("pause_s", 0.8))

    raw = fetch_randstad_it(base=base, query=query, max_pages=max_pages, pause=pause, debug=debug)
    if debug: print(f"RANDSTAD_IT RAW {len(raw)}")

    pre = len(raw)
    stage_kw = [x for x in raw if keyword_pass(x, source_cfg)]
    stage_loc = [x for x in stage_kw if location_pass(x, source_cfg)]
    stage_dt = [x for x in stage_loc if recency_pass(x, source_cfg)]

    if debug:
        print(f"SMART_FILTER Randstad_IT: pre={pre} kw={len(stage_kw)} loc={len(stage_loc)} date={len(stage_dt)}")

    return stage_dt
    
def fetch(cfg):
    # broad scrape; global filtering happens later in normalize/filter_jobs/dedupe
    base = cfg.get("base_url", "https://www.randstad.it/offerte-lavoro/")
    query = cfg.get("query")
    max_pages = int(cfg.get("max_pages", 10))
    pause = float(cfg.get("pause_s", 0.8))
    debug = bool(cfg.get("debug", False))
    return fetch_randstad_it(base=base, query=query, max_pages=max_pages, pause=pause, debug=debug)
