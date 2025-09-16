import os
import json
from typing import Dict, List, Any, Iterable, Tuple
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Workday CxS JSON endpoint patterns we will try:
#   A) https://{host}/wday/cxs/{tenant}/{site}/jobs?limit=200&offset=0
#   B) https://{host}/wday/cxs/{tenant}/jobs?limit=200&offset=0     (no site)

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://myworkdayjobs.com",
    "Referer": "https://myworkdayjobs.com/",
}

DEFAULT_SITES = ["External", "Global", "Careers", "Jobs", "Job", "JobBoard"]

LOCALE_PARAM_SETS = [
    {},  # none
    {"lang": "en-US"},
    {"lang": "en_GB"},
    {"locale": "en_US"},
    {"locale": "en-GB"},
]

LIMIT = 200

class HttpRetriableError(Exception):
    pass

def _on() -> bool:
    v = os.getenv("WORKDAY_DEBUG", "").strip().lower()
    return v in ("1", "true", "yes", "on")

def _norm(s: Any) -> str:
    if s is None: return ""
    if isinstance(s, (int, float)): return str(s)
    return str(s).strip()

def _join(parts: List[str], sep=", ") -> str:
    parts = [p for p in parts if p]
    return sep.join(parts)

def _parse_items(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict): return []
    for key in ("jobPostings", "items", "value", "postings", "data"):
        v = data.get(key)
        if isinstance(v, list) and v:
            return v
    body = data.get("body") or data.get("result")
    if isinstance(body, dict):
        for key in ("jobPostings", "items", "value", "postings", "data"):
            v = body.get(key)
            if isinstance(v, list) and v:
                return v
    return []

def _extract_location(obj: Dict[str, Any]) -> str:
    locs = obj.get("locations")
    if isinstance(locs, list) and locs:
        pieces = []
        for l in locs:
            if not isinstance(l, dict): continue
            city = _norm(l.get("city") or l.get("cityName") or l.get("cityText"))
            region = _norm(l.get("region") or l.get("state") or l.get("province"))
            country = _norm(l.get("country") or l.get("countryName") or l.get("countryCode"))
            pieces.append(_join([city, region, country]))
        s = "; ".join([p for p in pieces if p])
        if s: return s

    loc = obj.get("location") or obj.get("primaryLocation")
    if isinstance(loc, dict):
        city = _norm(loc.get("city") or loc.get("cityName"))
        region = _norm(loc.get("region") or loc.get("state"))
        country = _norm(loc.get("country") or loc.get("countryName") or loc.get("countryCode"))
        s = _join([city, region, country])
        if s: return s
        label = _norm(loc.get("label") or loc.get("name") or loc.get("displayName"))
        if label: return label
    elif isinstance(loc, str) and loc.strip():
        return loc.strip()

    for k in ("locationText", "city", "state", "country", "countryCode"):
        v = _norm(obj.get(k))
        if v: return v
    return ""

def _extract_url(obj: Dict[str, Any]) -> str:
    for k in ("externalUrl", "applyUrl", "url"):
        v = _norm(obj.get(k))
        if v: return v
    v = _norm(obj.get("externalPath"))
    if v: return v
    links = obj.get("links")
    if isinstance(links, list):
        for ln in links:
            if isinstance(ln, dict):
                href = _norm(ln.get("href"))
                if href: return href
    return ""

def _extract_title(obj: Dict[str, Any]) -> str:
    for k in ("title", "jobPostingTitle", "name", "displayTitle"):
        v = _norm(obj.get(k))
        if v: return v
    return _norm(obj.get("requisitionTitle"))

def _extract_id(obj: Dict[str, Any]) -> str:
    for k in ("id", "jobPostingId", "requisitionId", "jobReqId", "number"):
        v = _norm(obj.get(k))
        if v: return v
    return ""

def _extract_snippet(obj: Dict[str, Any]) -> str:
    for k in ("jobPostingDescription", "jobDescription", "description", "summary"):
        v = obj.get(k)
        if isinstance(v, str): return v[:240]
        if isinstance(v, dict):
            t = v.get("text") or v.get("html") or v.get("value")
            if isinstance(t, str): return t[:240]
    return ""

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(5),
       retry=retry_if_exception_type(HttpRetriableError))
def _get(client: httpx.Client, url: str, params: Dict[str, Any]) -> httpx.Response:
    r = client.get(url, params=params)
    if r.status_code == 429 or (500 <= r.status_code < 600):
        raise HttpRetriableError(f"{r.status_code} for {url}")
    return r

def _site_candidates(company: Dict[str, Any]) -> List[str]:
    sites: List[str] = []
    user_sites = company.get("workday_sites") or company.get("sites")
    if isinstance(user_sites, list) and user_sites:
        sites.extend([s for s in user_sites if isinstance(s, str) and s.strip()])

    name = (company.get("name") or "").lower()
    if "universal" in name or "umg" in name:
        for s in ("UMGUS", "UMGUK", "universal-music-group", "UNIVERSAL-MUSIC-GROUP"):
            if s not in sites: sites.append(s)
    if "warner" in name or "wmg" in name:
        for s in ("WMGUS", "WMGGLOBAL", "WMG", "Wmg"):
            if s not in sites: sites.append(s)

    for s in DEFAULT_SITES:
        if s not in sites: sites.append(s)
    return sites

def _hosts(company: Dict[str, Any]) -> List[str]:
    """
    Accept either a single host 'workday_host' or a list 'workday_hosts'.
    If none provided, derive a reasonable set of shard candidates.
    """
    out: List[str] = []
    hosts = company.get("workday_hosts")
    if isinstance(hosts, list) and hosts:
        out.extend([h.strip() for h in hosts if isinstance(h, str) and h.strip()])

    host = company.get("workday_host")
    if isinstance(host, str) and host.strip():
        if host.strip() not in out:
            out.append(host.strip())

    tenant = (company.get("workday_tenant") or "").strip().lower()
    # Common shard guesses:
    guesses = []
    if tenant:
        guesses.extend([
            f"{tenant}.wd1.myworkdayjobs.com",
            f"{tenant}.wd2.myworkdayjobs.com",
            f"{tenant}.wd3.myworkdayjobs.com",
            f"{tenant}.wd5.myworkdayjobs.com",
        ])
    # De-dup, preserve order
    for g in guesses:
        if g not in out:
            out.append(g)
    # Fallback:
    if not out:
        out.append("myworkdayjobs.com")
    return out

def _paths(tenant: str, site: str) -> List[str]:
    """
    Return both path shapes to try, with and without the site segment.
    """
    return [
        f"/wday/cxs/{tenant}/{site}/jobs",
        f"/wday/cxs/{tenant}/jobs",
    ]

def _map_item(host: str, company: Dict[str, Any], itm: Dict[str, Any]) -> Dict[str, Any]:
    jid = _extract_id(itm)
    title = _extract_title(itm)
    loc = _extract_location(itm)
    url2 = _extract_url(itm)
    if url2 and url2.startswith("/"):
        url2 = f"https://{host}{url2}"
    posted = _norm(itm.get("postedOn") or itm.get("startDate") or itm.get("postedDate"))
    return {
        "source": "workday",
        "company": company.get("name"),
        "id": jid or None,
        "title": title,
        "location": loc,
        "remote": bool(loc and ("remote" in loc.lower())),
        "department": _norm(itm.get("department") or itm.get("jobFamily") or itm.get("category")),
        "team": None,
        "url": url2 or None,
        "posted_at": posted or None,
        "description_snippet": _extract_snippet(itm),
    }

def fetch(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    if (company.get("ats") or "").lower() != "workday":
        return []

    debug = _on()
    attempts: List[Dict[str, Any]] = []
    out: List[Dict[str, Any]] = []

    tenant = (company.get("workday_tenant") or "").strip()
    if not tenant:
        return []

    hosts = _hosts(company)
    sites = _site_candidates(company)

    with httpx.Client(headers=BROWSER_HEADERS, timeout=30.0, follow_redirects=True) as client:
        for host in hosts:
            for site in sites:
                for path in _paths(tenant, site):
                    # page across locales
                    offset = 0
                    while True:
                        page_got = 0
                        for params in LOCALE_PARAM_SETS:
                            p = {"limit": LIMIT, "offset": offset}
                            p.update(params)
                            url = f"https://{host}{path}"
                            try:
                                r = _get(client, url, p)
                            except HttpRetriableError:
                                attempts.append({"u": url, "p": p, "s": "5xx/429", "items": 0})
                                continue
                            except Exception as e:
                                # capture the exception text so we can see WHY it says "error"
                                attempts.append({"u": url, "p": p, "s": "error", "err": str(e), "items": 0})
                                continue

                            ct = (r.headers.get("Content-Type") or "").lower()
                            if r.status_code != 200 or "json" not in ct:
                                attempts.append({"u": url, "p": p, "s": r.status_code, "items": 0})
                                continue

                            data = r.json()
                            items = _parse_items(data)
                            if not isinstance(items, list) or not items:
                                attempts.append({"u": url, "p": p, "s": r.status_code, "items": 0})
                                continue

                            mapped = [_map_item(host, company, itm) for itm in items if isinstance(itm, dict)]
                            out.extend(mapped)
                            page_got = len(mapped)
                            attempts.append({"u": url, "p": p, "s": r.status_code, "items": page_got})

                            if page_got == LIMIT:
                                continue  # next page with same locale
                            break  # break locale loop

                        if page_got < LIMIT:
                            break  # stop paging this path
                        offset += LIMIT

    if debug:
        attempts_str = json.dumps(attempts)[:2000]
        print(f"WORKDAY_DEBUG {company.get('name')}: tried={attempts_str} got={len(out)}")

    return out
