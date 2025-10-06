# adapters/workday_pw.py
import os, json, time
from typing import List, Dict, Any, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

WD_LIMIT         = int(os.getenv("WD_LIMIT", "200"))
WD_MAX_PAGES     = int(os.getenv("WD_MAX_PAGES", "2"))       # how many "load more"/next cycles per site
WD_EARLY_BREAK   = os.getenv("WD_EARLY_BREAK", "1") == "1"   # stop early if a page yields 0
WD_MAX_HOSTS     = int(os.getenv("WD_MAX_HOSTS", "2"))
WD_MAX_SITES     = int(os.getenv("WD_MAX_SITES", "6"))
PW_HEADLESS      = os.getenv("PW_HEADLESS", "1") == "1"
DEBUG            = os.getenv("WORKDAY_PW_DEBUG", "0") == "1"
LISTEN_WINDOW_MS = int(os.getenv("WD_LISTEN_MS", "6000"))    # how long to listen for /graphql after actions

def _debug(msg: str):
    if DEBUG:
        print(msg, flush=True)

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def _extract_from_graphql(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data") or {}
    nodes: List[Dict[str, Any]] = []

    def grab(js):
        if isinstance(js, dict):
            items = js.get("items") or []
            for it in items:
                if isinstance(it, dict):
                    nodes.append(it)

    # common shapes
    grab(data.get("jobSearch"))
    site = data.get("site")
    if isinstance(site, dict):
        grab(site.get("jobSearch"))

    if not nodes:
        # permissive deep walk
        def walk(obj):
            if isinstance(obj, dict):
                if "jobSearch" in obj and isinstance(obj["jobSearch"], dict):
                    its = obj["jobSearch"].get("items") or []
                    for it in its:
                        if isinstance(it, dict):
                            yield it
                for v in obj.values():
                    yield from walk(v)
            elif isinstance(obj, list):
                for v in obj:
                    yield from walk(v)
        nodes = list(walk(data))
    return nodes

def _make_job(company: str, host: str, site: str, node: Dict[str, Any]) -> Dict[str, Any]:
    title = _norm(node.get("title"))
    path  = node.get("externalPath") or ""
    url   = f"https://{host}{path}" if path.startswith("/") else f"https://{host}/{path}"
    locations = node.get("locationsText") or ", ".join(
        [l.get("name","") for l in (node.get("locations") or []) if isinstance(l, dict) and l.get("name")]
    )
    return {
        "company": company,
        "source": "workday",
        "title": title,
        "url": url,
        "location": _norm(locations),
        "department": "",
        "date_posted": node.get("postedOn") or "",
        "raw": node,
        "meta": {"workday_site": site, "host": host},
    }

def _listen_graphql(page, host: str, window_ms: int) -> List[Dict[str, Any]]:
    """Collect /graphql JSON responses for a short window."""
    collected: List[Dict[str, Any]] = []

    def on_response(resp):
        try:
            if "/graphql" in resp.url and host in resp.url and resp.request.method == "POST":
                # try json; fall back to text->json
                try:
                    payload = resp.json()
                except Exception:
                    txt = resp.text()
                    payload = json.loads(txt) if txt else {}
                nodes = _extract_from_graphql(payload)
                if nodes:
                    collected.extend(nodes)
        except Exception:
            pass  # keep listening

    page.on("response", on_response)
    try:
        time.sleep(max(0, window_ms) / 1000.0)
    finally:
        try:
            page.remove_listener("response", on_response)
        except Exception:
            pass
    return collected

def _accept_cookies(page):
    sels = [
        '#onetrust-accept-btn-handler',
        'button[aria-label="Accept all"]',
        'button:has-text("Accept")',
        'button:has-text("I Accept")',
        'button:has-text("Allow all")',
        'button:has-text("Accetta")',
        'button:has-text("Acconsento")',
    ]
    for sel in sels:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=1500)
                time.sleep(0.2)
        except Exception:
            continue

def _load_and_capture(page, url: str, host: str, site: str, company: str) -> List[Dict[str, Any]]:
    _debug(f"WORKDAY_PW_DEBUG load {url}")
    jobs: List[Dict[str, Any]] = []
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        _accept_cookies(page)
        # force hydration/xhr
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        nodes = _listen_graphql(page, host, LISTEN_WINDOW_MS)

        for n in nodes:
            jobs.append(_make_job(company, host, site, n))

        # try to paginate a bit
        clicks = 0
        while clicks < max(0, WD_MAX_PAGES - 1):
            did = False
            for sel in [
                'button:has-text("Load more")',
                'button:has-text("Show more")',
                'button:has-text("Mostra altro")',
                'button[aria-label="Load more results"]',
                'button:has-text("Next")',
                'a[aria-label="Next"]',
            ]:
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

            # listen again after the click
            nodes = _listen_graphql(page, host, LISTEN_WINDOW_MS)
            if not nodes and WD_EARLY_BREAK:
                break
            for n in nodes:
                jobs.append(_make_job(company, host, site, n))
            clicks += 1

    except PWTimeout as e:
        _debug(f"WORKDAY_PW_DEBUG timeout on {url}: {e}")
    except Exception as e:
        _debug(f"WORKDAY_PW_DEBUG error on {url}: {e}")

    return jobs

def fetch(company_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Required in company_cfg:
      - name
      - ats: workday_pw
      - workday_pw_hosts: [ 'umusic.wd5.myworkdayjobs.com', ... ]
      - workday_pw_sites: [ 'UMGUS', 'UMGUK', 'External', ... ]
    """
    name  = company_cfg.get("name", "WorkdayCompany")
    hosts = (company_cfg.get("workday_pw_hosts") or [])[:WD_MAX_HOSTS] if WD_MAX_HOSTS > 0 else (company_cfg.get("workday_pw_hosts") or [])
    sites = (company_cfg.get("workday_pw_sites") or [])[:WD_MAX_SITES] if WD_MAX_SITES > 0 else (company_cfg.get("workday_pw_sites") or [])

    got: List[Dict[str, Any]] = []
    tried: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PW_HEADLESS)
        ctx = browser.new_context()
        page = ctx.new_page()

        for h in hosts:
            tenant = h.split('.')[0]  # e.g. umusic
            for s in sites:
                url = f"https://{h}/wday/cxs/{tenant}/{s}"
                jobs = _load_and_capture(page, url, h, s, name)
                tried.append({"host": h, "site": s, "url": url, "status": "ok", "items": len(jobs)})
                got.extend(jobs)
                if WD_LIMIT and len(got) >= WD_LIMIT:
                    got = got[:WD_LIMIT]
                    break
            if WD_LIMIT and len(got) >= WD_LIMIT:
                break

        ctx.close()
        browser.close()

    if DEBUG:
        print(f"WORKDAY_PW_DEBUG {name}: tried={tried} got={len(got)}", flush=True)

    return got
