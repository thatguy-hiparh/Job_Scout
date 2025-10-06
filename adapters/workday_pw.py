# adapters/workday_pw.py
import os, json, re, time
from typing import List, Dict, Any, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

WD_LIMIT       = int(os.getenv("WD_LIMIT", "200"))
WD_MAX_PAGES   = int(os.getenv("WD_MAX_PAGES", "2"))         # pagination cycles per site
WD_EARLY_BREAK = os.getenv("WD_EARLY_BREAK", "1") == "1"     # stop a site when a page yields 0
WD_MAX_HOSTS   = int(os.getenv("WD_MAX_HOSTS", "2"))         # max hosts to try per company
WD_MAX_SITES   = int(os.getenv("WD_MAX_SITES", "6"))         # max site slugs per host
PW_HEADLESS    = os.getenv("PW_HEADLESS", "1") == "1"
DEBUG          = os.getenv("WORKDAY_PW_DEBUG", "0") == "1"

JOB_KEYS = [
    # Workday GraphQL payload “jobSearch” typical fields we care about
    "title", "externalPath", "locationsText", "timeType", "workerSubType",
    "postedOn", "locations", "requisitionId", "bulletfields", "id"
]

def _debug(msg: str):
    if DEBUG:
        print(msg, flush=True)

def _norm_text(s: Optional[str]) -> str:
    return (s or "").strip()

def _make_job(tenant: str, site: str, host: str, node: Dict[str, Any]) -> Dict[str, Any]:
    title = _norm_text(node.get("title"))
    path  = node.get("externalPath") or ""
    url   = f"https://{host}{path}" if path.startswith("/") else f"https://{host}/{path}"
    locations = node.get("locationsText") or ", ".join([l.get("name","") for l in (node.get("locations") or []) if l.get("name")])
    return {
        "company": tenant.upper(),
        "source": "workday",
        "title": title,
        "url": url,
        "location": _norm_text(locations),
        "department": "",
        "date_posted": node.get("postedOn") or "",
        "raw": node,
        "meta": {"workday_site": site, "host": host},
    }

def _extract_from_graphql(resp_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the list of jobSearch nodes from a /graphql response."""
    # Workday structures vary; look for any path that contains 'jobSearch'
    data = resp_json.get("data") or {}
    # Common shapes:
    # data > jobSearch > items
    # data > site > jobSearch > items
    nodes = []
    js = data.get("jobSearch")
    if isinstance(js, dict):
        items = js.get("items") or []
        nodes.extend(items)

    site = data.get("site")
    if isinstance(site, dict):
        js2 = site.get("jobSearch")
        if isinstance(js2, dict):
            items2 = js2.get("items") or []
            nodes.extend(items2)

    # Some tenants use arrays of sections; be permissive
    if not nodes:
        # try to find any dict with 'jobSearch' key deep-ish
        def walk(obj):
            if isinstance(obj, dict):
                if "jobSearch" in obj and isinstance(obj["jobSearch"], dict):
                    its = obj["jobSearch"].get("items") or []
                    for it in its:
                        yield it
                for v in obj.values():
                    yield from walk(v)
            elif isinstance(obj, list):
                for v in obj:
                    yield from walk(v)
        nodes = list(walk(data))
    return nodes

def _watch_graphql(page, host: str, site: str, offset: int) -> List[Dict[str, Any]]:
    """Wait for /graphql response and return parsed jobs."""
    jobs = []

    def is_target(r):
        return (
            r.request.method == "POST"
            and "/graphql" in r.url
            and host in r.url
        )

    # Clear any backlog by waiting briefly
    try:
        resp = page.wait_for_response(is_target, timeout=15000)
        payload = {}
        try:
            payload = resp.json()
        except Exception:
            ct = resp.headers.get("content-type", "")
            txt = resp.text() if "json" not in ct else "{}"
            try:
                payload = json.loads(txt)
            except Exception:
                payload = {}
        nodes = _extract_from_graphql(payload)
        if nodes:
            for n in nodes:
                jobs.append(_make_job("workday", site, host, n))
    except PWTimeout:
        pass
    return jobs

def _accept_cookies(page):
    # Best-effort clicks for cookie banners commonly seen on Workday tenants
    selectors = [
        'button:has-text("Accept")',
        'button:has-text("I Accept")',
        'button:has-text("Allow all")',
        'button[aria-label="Accept all"]',
        '#onetrust-accept-btn-handler',
        'button:has-text("Acconsento")',
        'button:has-text("Accetta")',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                el.first.click(timeout=2000)
                time.sleep(0.2)
        except Exception:
            continue

def _load_and_listen(page, url: str, host: str, site: str) -> List[Dict[str, Any]]:
    """Open a site page and capture one or more /graphql responses."""
    _debug(f"WORKDAY_PW_DEBUG load {url}")
    jobs = []
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        _accept_cookies(page)
        # Trigger scroll to force XHRs/hydration
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        # Listen for first batch
        batch = _watch_graphql(page, host, site, offset=0)
        jobs.extend(batch)

        # Try to click “Load more”/pagination buttons while under page caps
        clicked = 0
        while clicked < max(0, WD_MAX_PAGES - 1):
            # Try common load-more buttons
            candidates = [
                'button:has-text("Load more")',
                'button:has-text("Show more")',
                'button:has-text("Mostra altro")',
                'button[aria-label="Load more results"]',
                'button:has-text("Next")',
                'a[aria-label="Next"]',
            ]
            did = False
            for sel in candidates:
                try:
                    btn = page.locator(sel)
                    if btn.count() > 0 and btn.first.is_enabled():
                        btn.first.click(timeout=2000)
                        did = True
                        break
                except Exception:
                    continue
            if not did:
                break
            # wait for the next graphql burst
            batch = _watch_graphql(page, host, site, offset=(clicked + 1))
            if not batch and WD_EARLY_BREAK:
                break
            jobs.extend(batch)
            clicked += 1

    except Exception as e:
        _debug(f"WORKDAY_PW_DEBUG error on {url}: {e}")

    return jobs

def fetch(company_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    company_cfg must contain:
      - name
      - ats: "workday_pw"
      - workday_pw_hosts: [ "umusic.wd5.myworkdayjobs.com", ... ]
      - workday_pw_sites: [ "UMGUS", "UMGUK", "External", ... ]
    """
    name = company_cfg.get("name", "WorkdayCompany")
    hosts = company_cfg.get("workday_pw_hosts") or []
    sites = company_cfg.get("workday_pw_sites") or []

    got = []
    tried = []

    hosts = hosts[:WD_MAX_HOSTS] if WD_MAX_HOSTS > 0 else hosts
    sites = sites[:WD_MAX_SITES] if WD_MAX_SITES > 0 else sites

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PW_HEADLESS)
        ctx = browser.new_context()
        page = ctx.new_page()

        for h in hosts:
            for s in sites:
                base = f"https://{h}/wday/cxs/{h.split('.')[0] if h.endswith('myworkdayjobs.com') else 'tenant'}/{s}"
                # NOTE: many tenants are actually “/wday/cxs/<tenant>/SITENAME” where tenant == subdomain before first dot
                # For example: umusic.wd5.myworkdayjobs.com/wday/cxs/umusic/UMGUS
                # So derive tenant from host: "umusic"
                tenant = h.split('.')[0]
                url = f"https://{h}/wday/cxs/{tenant}/{s}"
                jobs = _load_and_listen(page, url, h, s)
                tried.append({"host": h, "site": s, "url": url, "status": "ok", "items": len(jobs)})
                got.extend(jobs)
                if len(got) >= WD_LIMIT:
                    got = got[:WD_LIMIT]
                    break
            if len(got) >= WD_LIMIT:
                break

        ctx.close()
        browser.close()

    if DEBUG:
        print(f"WORKDAY_PW_DEBUG {name}: tried={tried} got={len(got)}", flush=True)

    # Re-tag company properly now that we know it:
    for j in got:
        j["company"] = name

    return got
