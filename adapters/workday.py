import os
import json
from typing import Dict, List, Any
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Workday CxS JSON endpoint pattern:
#   https://{host}/wday/cxs/{tenant}/{site}/jobs?limit=200&offset=0

DEFAULT_HEADERS = {
    "User-Agent": "job-scout/1.0 (+https://github.com/thatguy-hiparh/Job_Scout)",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://myworkdayjobs.com",
    "Referer": "https://myworkdayjobs.com/",
}

DEFAULT_SITES = [
    "External",
    "Global",
    "Careers",
    "Jobs",
    "Job",
    "JobBoard",
]

LOCALE_PARAM_SETS = [
    {},  # no locale
    {"lang": "en-US"},
    {"lang": "en_GB"},
    {"locale": "en_US"},
    {"locale": "en-GB"},
]

LIMIT = 200

class HttpRetriableError(Exception):
    pass

def _debug_on() -> bool:
    v = os.getenv("WORKDAY_DEBUG", "").strip().lower()
    return v in ("1", "true", "yes", "on")

def _safe_get(client: httpx.Client, url: str, params: Dict[str, Any]) -> httpx.Response:
    r = client.get(url, params=params)
    if r.status_code == 429 or (500 <= r.status_code < 600):
        raise HttpRetriableError(f"{r.status_code} for {url}")
    return r

@retry(
    wait=wait_exponential(min=1, max=20),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(HttpRetriableError),
)
def _get(client: httpx.Client, url: str, params: Dict[str, Any]) -> httpx.Response:
    return _safe_get(client, url, params)

def _norm(s: Any) -> str:
    if s is None:
        return ""
    if isinstance(s, (int, float)):
        return str(s)
    return str(s).strip()

def _join_nonempty(parts: List[str], sep=", ") -> str:
    parts = [p for p in parts if p]
    return sep.join(parts)

def _parse_items(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
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
        parts = []
        for l in locs:
            if not isinstance(l, dict):
                continue
            city = _norm(l.get("city") or l.get("cityName") or l.get("cityText"))
            region = _norm(l.get("region") or l.get("state") or l.get("province"))
            country = _norm(l.get("country") or l.get("countryName") or l.get("countryCode"))
            parts.append(_join_nonempty([city, region, country]))
        s = "; ".join([p for p in parts if p])
        if s:
            return s

    loc = obj.get("location") or obj.get("primaryLocation")
    if isinstance(loc, dict):
        city = _norm(loc.get("city") or loc.get("cityName"))
        region = _norm(loc.get("region") or loc.get("state"))
        country = _norm(loc.get("country") or loc.get("countryName") or loc.get("countryCode"))
        s = _join_nonempty([city, region, country])
        if s:
            return s
        label = _norm(loc.get("label") or loc.get("name") or loc.get("displayName"))
        if label:
            return label
    elif isinstance(loc, str):
        if loc.strip():
            return loc.strip()

    for k in ("locationText", "city", "state", "country", "countryCode"):
        v = _norm(obj.get(k))
        if v:
            return v
    return ""

def _extract_url(obj: Dict[str, Any]) -> str:
    for k in ("externalUrl", "applyUrl", "url"):
        v = _norm(obj.get(k))
        if v:
            return v
    v = _norm(obj.get("externalPath"))
    if v:
        return v
    links = obj.get("links")
    if isinstance(links, list):
        for ln in links:
            if isinstance(ln, dict) and _norm(ln.get("href")):
                return _norm(ln.get("href"))
    return ""

def _extract_title(obj: Dict[str, Any]) -> str:
    for k in ("title", "jobPostingTitle", "name", "displayTitle"):
        v = _norm(obj.get(k))
        if v:
            return v
    return _norm(obj.get("requisitionTitle"))

def _extract_id(obj: Dict[str, Any]) -> str:
    for k in ("id", "jobPostingId", "requisitionId", "jobReqId"):
        v = _norm(obj.get(k))
        if v:
            return v
    v = _norm(obj.get("number"))
    if v:
        return v
    return ""

def _extract_snippet(obj: Dict[str, Any]) -> str:
    for k in ("jobPostingDescription", "jobDescription", "description", "summary"):
        v = obj.get(k)
        if isinstance(v, str):
            return v[:240]
        if isinstance(v, dict):
            t = v.get("text") or v.get("html") or v.get("value")
            if isinstance(t, str):
                return t[:240]
    return ""

def _build_base(host: str, tenant: str, site: str) -> str:
    return f"https://{host}/wday/cxs/{tenant}/{site}/jobs"

def _site_candidates(company: Dict[str, Any]) -> List[str]:
    sites: List[str] = []
    user_sites = company.get("workday_sites") or company.get("sites")
    if isinstance(user_sites, list) and user_sites:
        sites.extend([s for s in user_sites if isinstance(s, str) and s.strip()])

    name = (company.get("name") or "").lower()
    if "universal" in name or "umg" in name:
        for s in ("UMGUS", "UMGUK", "universal-music-group", "UNIVERSAL-MUSIC-GROUP"):
            if s not in sites:
                sites.append(s)
    if "warner" in name or "wmg" in name:
        for s in ("WMGUS", "WMGGLOBAL", "WMG", "Wmg"):
            if s not in sites:
                sites.append(s)

    for s in DEFAULT_SITES:
        if s not in sites:
            sites.append(s)
    return sites

def _host_for(company: Dict[str, Any]) -> str:
    host = company.get("workday_host")
    if isinstance(host, str) and host.strip():
        return host.strip()
    tenant = company.get("workday_tenant") or ""
    t = tenant.strip().lower()
    if t == "umusic":
        return "umusic.wd5.myworkdayjobs.com"
    if t == "wmg":
        return "wmg.wd1.myworkdayjobs.com"
    return f"{t}.wd5.myworkdayjobs.com" if t else "myworkdayjobs.com"

def fetch(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    if (company.get("ats") or "").lower() != "workday":
        return []

    debug = _debug_on()
    attempts: List[Dict[str, Any]] = []
    out: List[Dict[str, Any]] = []

    tenant = (company.get("workday_tenant") or "").strip()
    if not tenant:
        return []

    host = _host_for(company)
    sites = _site_candidates(company)

    with httpx.Client(headers=DEFAULT_HEADERS, timeout=30.0, follow_redirects=True) as client:
        for site in sites:
            offset = 0
            while True:
                page_got = 0
                for params in LOCALE_PARAM_SETS:
                    p = {"limit": LIMIT, "offset": offset}
                    p.update(params)
                    url = _build_base(host, tenant, site)
                    try:
                        r = _get(client, url, p)
                    except HttpRetriableError:
                        attempts.append({"u": url, "p": p, "s": "5xx/429", "items": 0})
                        continue
                    except Exception:
                        attempts.append({"u": url, "p": p, "s": "error", "items": 0})
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

                    mapped = []
                    for itm in items:
                        if not isinstance(itm, dict):
                            continue
                        jid = _extract_id(itm)
                        title = _extract_title(itm)
                        loc = _extract_location(itm)
                        url2 = _extract_url(itm)
                        if url2 and url2.startswith("/"):
                            url2 = f"https://{host}{url2}"
                        posted = _norm(itm.get("postedOn") or itm.get("startDate") or itm.get("postedDate"))
                        mapped.append({
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
                        })

                    out.extend(mapped)
                    page_got = len(mapped)
                    attempts.append({"u": url, "p": p, "s": r.status_code, "items": page_got})

                    if page_got == LIMIT:
                        continue
                    break  # break locale loop

                if page_got < LIMIT:
                    break  # stop paging this site
                offset += LIMIT

    if debug:
        attempts_str = json.dumps(attempts)[:1800]
        print(f"WORKDAY_DEBUG {company.get('name')}: tried={attempts_str} got={len(out)}")

    return out
