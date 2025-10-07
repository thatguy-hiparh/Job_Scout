# adapters/workday_pw.py
import os, json, math
from typing import List, Dict
from playwright.sync_api import sync_playwright

WD_LIMIT       = int(os.getenv("WD_LIMIT", "200"))
WD_MAX_PAGES   = int(os.getenv("WD_MAX_PAGES", "2"))
WD_EARLY_BREAK = os.getenv("WD_EARLY_BREAK", "1") == "1"
WD_MAX_HOSTS   = int(os.getenv("WD_MAX_HOSTS", "2"))
WD_MAX_SITES   = int(os.getenv("WD_MAX_SITES", "6"))
PW_HEADLESS    = os.getenv("PW_HEADLESS", "1") == "1"
DEBUG          = os.getenv("WORKDAY_PW_DEBUG", "0") == "1"

# Minimal GraphQL that many Workday tenants expose via CXS
GRAPHQL_QUERY = """
query SearchJobs($limit: Int!, $offset: Int!) {
  jobSearch(criteria: {}) {
    totalCount
    results(limit: $limit, offset: $offset) {
      title
      externalPath
      locations { city region country }
      postedDate
    }
  }
}
"""

def _log(s: str):
    if DEBUG: print(s)

def _norm_job(host: str, site: str, j: Dict) -> Dict:
    # normalize a Workday job from either graphql or /jobs JSON
    title = j.get("title") or j.get("titleLocalized") or ""
    path  = j.get("externalPath") or j.get("externalPathName") or j.get("externalPathNameLocalized") or ""
    url   = f"https://{host}/{site}{path if path.startswith('/') else '/' + path}" if path else f"https://{host}/{site}"
    locs  = j.get("locations") or []
    # best-effort location string
    loc_txt = ""
    if isinstance(locs, list) and locs:
        loc0 = locs[0]
        loc_txt = ", ".join([v for v in [loc0.get("city"), loc0.get("region"), loc0.get("country")] if v])
    elif isinstance(locs, dict):
        loc_txt = ", ".join([v for v in [locs.get("city"), locs.get("region"), locs.get("country")] if v])

    return {
        "source": "workday",
        "company": host.split(".")[0].upper(),
        "title": title,
        "location": loc_txt,
        "url": url,
    }

def _inpage_fetch_graphql(page, host: str, site: str, limit: int, offset: int):
    url = f"https://{host}/wday/cxs/{host.split('.')[0]}/{site}/graphql"
    payload = {
        "operationName": "SearchJobs",
        "query": GRAPHQL_QUERY,
        "variables": {"limit": limit, "offset": offset},
    }
    _log(f"WORKDAY_PW_DEBUG POST {url} -> (in-page)")
    return page.evaluate(
        """async ({url, payload}) => {
            const res = await fetch(url, {
              method: 'POST',
              headers: {'content-type': 'application/json'},
              body: JSON.stringify(payload),
              credentials: 'include'
            });
            return {status: res.status, json: await res.json().catch(()=>null)};
        }""",
        {"url": url, "payload": payload},
    )

def _inpage_fetch_jobs(page, host: str, site: str, limit: int, offset: int, active_only=True):
    url = f"https://{host}/wday/cxs/{host.split('.')[0]}/{site}/jobs?limit={limit}&offset={offset}"
    if active_only:
        url += "&activeOnly=true"
    _log(f"WORKDAY_PW_DEBUG GET {url} -> (in-page)")
    return page.evaluate(
        """async (url) => {
            const res = await fetch(url, { credentials: 'include' });
            let data = null;
            try { data = await res.json(); } catch(e) {}
            return {status: res.status, json: data};
        }""",
        url,
    )

def _collect_from_site(context, host: str, site: str) -> List[Dict]:
    jobs: List[Dict] = []
    page = context.new_page()
    # Load the site first to receive first-party cookies
    root = f"https://{host}/{site}"
    _log(f"WORKDAY_PW_DEBUG load {root}")
    try:
        page.goto(root, wait_until="domcontentloaded", timeout=30000)
    except Exception as e:
        _log(f"WORKDAY_PW_DEBUG error loading {root}: {e}")
        page.close()
        return jobs

    # Try GraphQL first (more stable pagination)
    try:
        offset = 0
        pages  = 0
        first  = _inpage_fetch_graphql(page, host, site, min(WD_LIMIT, 200), offset)
        if first and first.get("status") == 200 and first.get("json"):
            data = first["json"].get("data") or {}
            block = (data.get("jobSearch") or {})
            total = int(block.get("totalCount") or 0)
            items = block.get("results") or []
            jobs.extend([_norm_job(host, site, j) for j in items])
            pages += 1
            while len(jobs) < total and pages < WD_MAX_PAGES:
                offset += len(items) if items else WD_LIMIT
                nxt = _inpage_fetch_graphql(page, host, site, min(WD_LIMIT, 200), offset)
                if not nxt or nxt.get("status") != 200 or not nxt.get("json"):
                    break
                data = nxt["json"].get("data") or {}
                items = (data.get("jobSearch") or {}).get("results") or []
                if not items:
                    break
                jobs.extend([_norm_job(host, site, j) for j in items])
                pages += 1
            page.close()
            return jobs
    except Exception as e:
        _log(f"WORKDAY_PW_DEBUG graphql fallback on {host}/{site}: {e}")

    # Fallback: /jobs JSON
    try:
        offset = 0
        pages  = 0
        first = _inpage_fetch_jobs(page, host, site, min(WD_LIMIT, 200), offset, active_only=True)
        if first and first.get("status") == 200 and first.get("json"):
            data  = first["json"]
            items = data.get("jobPostings") or data.get("jobPostingsPage") or data.get("jobPostingsV2") or data.get("jobPostingsV3") or []
            # Some tenants return {"items":[...], "total":N}
            if isinstance(data, dict) and "items" in data:
                items = data.get("items") or []
            jobs.extend([_norm_job(host, site, j) for j in items if isinstance(j, dict)])
            pages += 1
            while items and pages < WD_MAX_PAGES:
                offset += len(items)
                nxt = _inpage_fetch_jobs(page, host, site, min(WD_LIMIT, 200), offset, active_only=True)
                if not nxt or nxt.get("status") != 200 or not nxt.get("json"):
                    break
                data  = nxt["json"]
                items = data.get("jobPostings") or data.get("items") or []
                if not items:
                    break
                jobs.extend([_norm_job(host, site, j) for j in items if isinstance(j, dict)])
                pages += 1
    except Exception as e:
        _log(f"WORKDAY_PW_DEBUG jobs fallback on {host}/{site}: {e}")

    page.close()
    return jobs

def fetch(company: Dict) -> List[Dict]:
    """
    company.workday.hosts: [ 'umusic.wd5.myworkdayjobs.com', ... ]
    company.workday.sites: [ 'UMGUS', 'UMGUK', 'External', ... ]
    """
    cfg = company.get("workday") or {}
    hosts = cfg.get("hosts") or []
    sites = cfg.get("sites") or []

    if not hosts or not sites:
        return []

    out: List[Dict] = []
    tried = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PW_HEADLESS)
        context = browser.new_context()
        for hi, host in enumerate(hosts[:WD_MAX_HOSTS]):
            for si, site in enumerate(sites[:WD_MAX_SITES]):
                jobs = _collect_from_site(context, host, site)
                tried.append({"host": host, "site": site, "url": f"https://{host}/wday/cxs/{host.split('.')[0]}/{site}", "status": "ok", "items": len(jobs)})
                out.extend(jobs)
                if WD_EARLY_BREAK and jobs:
                    # stop after the first site that yields results
                    break
        browser.close()

    _log(f"WORKDAY_PW_DEBUG {company.get('name')}: tried={tried} got={len(out)}")
    return out
