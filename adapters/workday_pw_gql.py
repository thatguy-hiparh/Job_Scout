# adapters/workday_pw_gql.py
import os, json
from typing import List, Dict, Any, Optional
from playwright.sync_api import sync_playwright, APIResponse

WD_LIMIT         = int(os.getenv("WD_LIMIT", "200"))
WD_MAX_HOSTS     = int(os.getenv("WD_MAX_HOSTS", "2"))
WD_MAX_SITES     = int(os.getenv("WD_MAX_SITES", "6"))
PW_HEADLESS      = os.getenv("PW_HEADLESS", "1") == "1"
DEBUG            = os.getenv("WORKDAY_PW_DEBUG", "0") == "1"

def _dbg(msg: str):
    if DEBUG:
        print(msg, flush=True)

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def _extract_items(payload: Dict[str, Any]) -> list[dict]:
    """
    Extract items from Workday GraphQL response.
    """
    try:
        data = payload.get("data") or {}
        js = data.get("jobSearch") or {}
        items = js.get("items") or []
        if isinstance(items, list):
            return items
    except Exception:
        pass
    return []

def _build_job(company: str, host: str, tenant: str, site: str, node: dict) -> dict:
    title = _norm(node.get("title"))
    path  = node.get("externalPath") or ""
    url = f"https://{host}{path}" if path.startswith("/") else f"https://{host}/{path}"
    loc  = node.get("locationsText") or ", ".join(
        [l.get("name","") for l in (node.get("locations") or []) if isinstance(l, dict)]
    )
    return {
        "company": company,
        "source": "workday_gql",
        "title": title,
        "url": url,
        "location": _norm(loc),
        "department": "",
        "date_posted": node.get("postedOn") or "",
        "raw": node,
    }

GQL_QUERY = """
query jobSearch($limit:Int,$offset:Int,$activeOnly:Boolean){
  jobSearch(limit:$limit,offset:$offset,activeOnly:$activeOnly){
    items{
      title
      externalPath
      locationsText
      postedOn
      locations{name}
    }
  }
}
"""

def _post_jobs(ctx_request, url_gql: str, headers: dict) -> list[dict]:
    payload = {
        "operationName": "jobSearch",
        "variables": {"limit": 200, "offset": 0, "activeOnly": True},
        "query": GQL_QUERY,
    }
    try:
        resp: APIResponse = ctx_request.post(url_gql, data=json.dumps(payload),
                                             headers=headers, timeout=30000)
        if not resp.ok:
            _dbg(f"WORKDAY_PW_DEBUG POST {url_gql} -> {resp.status}")
            return []
        data = resp.json()
        items = _extract_items(data)
        _dbg(f"WORKDAY_PW_DEBUG POST {url_gql} -> items={len(items)}")
        return items
    except Exception as e:
        _dbg(f"WORKDAY_PW_DEBUG error POST {url_gql}: {e}")
        return []

def fetch(company_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    company_cfg needs:
      - name
      - ats: workday_pw_gql
      - workday_pw_hosts
      - workday_pw_sites
    """
    name  = company_cfg.get("name", "WorkdayCompany")
    hosts = (company_cfg.get("workday_pw_hosts") or [])[:WD_MAX_HOSTS]
    sites = (company_cfg.get("workday_pw_sites") or [])[:WD_MAX_SITES]

    gathered, tried_meta = [], []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PW_HEADLESS)
        ctx = browser.new_context()
        page = ctx.new_page()

        for host in hosts:
            tenant = host.split(".")[0]
            origin = f"https://{host}"
            for site in sites:
                url_page = f"{origin}/wday/cxs/{tenant}/{site}"
                url_gql  = f"{url_page}/graphql"

                try:
                    page.goto(url_page, wait_until="domcontentloaded", timeout=20000)
                except Exception as e:
                    _dbg(f"WORKDAY_PW_DEBUG warm {url_page}: {e}")

                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                    "Referer": url_page,
                    "Origin": origin,
                    "X-Requested-With": "XMLHttpRequest",
                    "Workday-Client": "workday+cxs",
                }

                items = _post_jobs(ctx.request, url_gql, headers)
                jobs  = [_build_job(name, host, tenant, site, n) for n in items]
                tried_meta.append({"host": host, "site": site, "url": url_gql, "status": "ok", "items": len(jobs)})
                gathered.extend(jobs)

                if WD_LIMIT and len(gathered) >= WD_LIMIT:
                    gathered = gathered[:WD_LIMIT]
                    break
            if WD_LIMIT and len(gathered) >= WD_LIMIT:
                break

        ctx.close()
        browser.close()

    if DEBUG:
        print(f"WORKDAY_PW_DEBUG {name}: tried={tried_meta} got={len(gathered)}", flush=True)
    return gathered