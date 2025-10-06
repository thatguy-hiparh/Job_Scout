# adapters/workday_pw.py
import os, json
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
    Workday /wday/cxs/{tenant}/{site}/jobs returns JSON in a few shapes.
    Try common paths, then deep-walk as a fallback.
    """
    if not isinstance(payload, dict):
        return []

    js = payload.get("jobSearch")
    if isinstance(js, dict) and isinstance(js.get("items"), list):
        return js["items"]

    data = payload.get("data") or {}
    js = data.get("jobSearch")
    if isinstance(js, dict) and isinstance(js.get("items"), list):
        return js["items"]

    out: List[Dict[str, Any]] = []
    def walk(o):
        if isinstance(o, dict):
            if "jobSearch" in o and isinstance(o["jobSearch"], dict):
                it = o["jobSearch"].get("items") or []
                for n in it:
                    if isinstance(n, dict):
                        out.append(n)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
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

def _find_xsrf_token(cookies: List[Dict[str, Any]]) -> Optional[str]:
    """
    Different tenants use different names; grab any cookie that looks like an XSRF token.
    Common: 'XSRF-TOKEN', 'WD-XSRF-TOKEN', 'WDAY-XSRF-TOKEN'
    """
    for c in cookies:
        name = (c.get("name") or "").lower()
        if "xsrf" in name:
            return c.get("value")
    return None

def _make_headers(origin: str, referer: str, xsrf: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "Workday-Client": "workday+cxs",
        "Referer": referer,
        "Origin": origin,
    }
    if xsrf:
        # Header name varies slightly between tenants, this one is widely accepted:
        headers["X-WD-XSRF-TOKEN"] = xsrf
    return headers

def _try_fetch_jobs(ctx_request, url_api: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Hit the JSON endpoint with multiple param variants.
    Stop on first non-empty result.
    """
    variants = [
        {"limit": 200, "offset": 0, "activeOnly": "true"},
        {"limit": 200, "offset": 0},
        {"limit": 100, "offset": 0, "activeOnly": "true"},
    ]
    for params in variants:
        try:
            resp: APIResponse = ctx_request.get(url_api, params=params, headers=headers, timeout=30000)
            if not resp.ok:
                _dbg(f"WORKDAY_PW_DEBUG GET {url_api} {params} -> {resp.status}")
                continue

            ctype = (resp.headers.get("content-type") or "").lower()
            if "json" in ctype:
                payload = resp.json()
            else:
                # Occasionally servers return text/html but the body is JSON
                txt = resp.text()
                try:
                    payload = json.loads(txt)
                except Exception:
                    _dbg(f"WORKDAY_PW_DEBUG non-JSON content for {url_api} {params}, len={len(txt)}")
                    continue

            items = _extract_items(payload)
            _dbg(f"WORKDAY_PW_DEBUG GET {url_api} {params} -> items={len(items)}")
            if items:
                return items
        except Exception as e:
            _dbg(f"WORKDAY_PW_DEBUG error GET {url_api} {params}: {e}")
    return []

def fetch(company_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    company_cfg requires:
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
            tenant = host.split(".")[0]
            origin = f"https://{host}"
            for site in sites:
                url_page = f"{origin}/wday/cxs/{tenant}/{site}"
                url_api  = f"{url_page}/jobs"

                # 1) Warm page (sets cookies & any session headers)
                try:
                    page.goto(url_page, wait_until="domcontentloaded", timeout=20000)
                except Exception as e:
                    _dbg(f"WORKDAY_PW_DEBUG warm {url_page}: {e}")

                # 2) Build headers using cookies (XSRF if present)
                cookies = ctx.cookies(url_page)
                xsrf = _find_xsrf_token(cookies)
                headers = _make_headers(origin, url_page, xsrf)

                # 3) Try to pull jobs JSON directly
                items = _try_fetch_jobs(ctx.request, url_api, headers)
                jobs = [_build_job(name, host, tenant, site, n) for n in items]
                tried_meta.append({"host": host, "site": site, "url": url_api, "status": "ok", "items": len(jobs)})
                gathered.extend(jobs)

                # Respect overall cap
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