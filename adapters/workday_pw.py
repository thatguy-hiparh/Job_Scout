# adapters/workday_pw.py
import os, json, time
from typing import List, Dict, Any, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, APIResponse

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

def _extract_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Workday /wday/cxs/{tenant}/{site}/jobs returns JSON.
    Common shapes:
      { "jobSearch": { "items": [ {...}, ... ] } }
      Or nested under "data" for some tenants.
    Be permissive.
    """
    if not isinstance(payload, dict):
        return []

    # direct common path
    js = payload.get("jobSearch")
    if isinstance(js, dict) and isinstance(js.get("items"), list):
        return js["items"]

    # sometimes wrapped
    data = payload.get("data") or {}
    js = data.get("jobSearch")
    if isinstance(js, dict) and isinstance(js.get("items"), list):
        return js["items"]

    # deep-walk fallback
    out: List[Dict[str, Any]] = []
    def walk(o):
        if isinstance(o, dict):
            if "jobSearch" in o and isinstance(o["jobSearch"], dict):
                items = o["jobSearch"].get("items") or []
                for it in items:
                    if isinstance(it, dict):
                        out.append(it)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o: walk(v)
    walk(payload)
    return out

def _build_job(company: str, host: str, tenant: str, site: str, node: Dict[str, Any]) -> Dict[str, Any]:
    title = _norm(node.get("title"))
    path  = node.get("externalPath") or ""
    if path.startswith("/"):
        url = f"https://{host}{path}"
    else:
        url = f"https://{host}/{path}" if path else f"https://{host}/wday/cxs/{tenant}/{site}"
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

def _try_fetch_jobs(ctx_request, url: str) -> List[Dict[str, Any]]:
    """
    Try several parameter variants commonly accepted by the endpoint.
    Stop on first non-empty result.
    """
    variants = [
        {"limit": 200, "offset": 0, "activeOnly": "true"},
        {"limit": 200, "offset": 0},
        {"limit": 100, "offset": 0, "activeOnly": "true"},
    ]
    for params in variants:
        try:
            resp: APIResponse = ctx_request.get(url, params=params, timeout=30000)
            if not resp.ok:
                _dbg(f"WORKDAY_PW_DEBUG GET {url} {params} -> {resp.status}")
                continue
            ctype = resp.headers.get("content-type", "")
            if "application/json" not in ctype and "json" not in ctype:
                # Some tenants return text/html unless XHR-like — but Playwright request is XHR enough.
                # Still, guard it.
                txt = resp.text()
                # Sometimes the JSON is embedded as plain text; try to parse.
                try:
                    payload = json.loads(txt)
                except Exception:
                    _dbg(f"WORKDAY_PW_DEBUG non-JSON content for {url} {params}, len={len(txt)}")
                    continue
            else:
                payload = resp.json()

            items = _extract_items(payload)
            _dbg(f"WORKDAY_PW_DEBUG {url} {params} -> items={len(items)}")
            if items:
                return items
        except Exception as e:
            _dbg(f"WORKDAY_PW_DEBUG error GET {url} {params}: {e}")
            continue
    return []

def fetch(company_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    company_cfg requirements:
      - name
      - ats: workday_pw
      - workday_pw_hosts: [ 'umusic.wd5.myworkdayjobs.com', ... ]
      - workday_pw_sites: [ 'UMGUS', 'UMGUK', 'External', ... ]
    """
    name  = company_cfg.get("name", "WorkdayCompany")
    hosts = (company_cfg.get("workday_pw_hosts") or [])[:WD_MAX_HOSTS] if WD_MAX_HOSTS > 0 else (company_cfg.get("workday_pw_hosts") or [])
    sites = (company_cfg.get("workday_pw_sites") or [])[:WD_MAX_SITES] if WD_MAX_SITES > 0 else (company_cfg.get("workday_pw_sites") or [])

    gathered: List[Dict[str, Any]] = []
    tried_meta: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PW_HEADLESS)
        ctx = browser.new_context()
        page = ctx.new_page()

        for host in hosts:
            tenant = host.split(".")[0]  # e.g. umusic, wmg
            for site in sites:
                # 1) Warm the context (cookies/CDP) by visiting the page quickly.
                url_page = f"https://{host}/wday/cxs/{tenant}/{site}"
                try:
                    page.goto(url_page, wait_until="domcontentloaded", timeout=20000)
                except Exception as e:
                    _dbg(f"WORKDAY_PW_DEBUG warm {url_page}: {e}")

                # 2) Hit the JSON endpoint directly via context.request
                url_api = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
                items = _try_fetch_jobs(ctx.request, url_api)

                jobs = [_build_job(name, host, tenant, site, n) for n in items]
                tried_meta.append({"host": host, "site": site, "url": url_api, "status": "ok", "items": len(jobs)})
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
