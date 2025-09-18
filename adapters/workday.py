import os
import json
import uuid
import time
from typing import Dict, List, Any
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# We will probe:
#   A) https://{host}/wday/cxs/{tenant}/{site}/jobs
#   B) https://{host}/wday/cxs/{tenant}/jobs

def _env_flag(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    return (str(v).strip().lower() in ("1","true","yes","on")) or (default and v == "")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default

def _env_list(name: str, default: List[str]) -> List[str]:
    v = os.getenv(name, "")
    if not v:
        return default
    parts = [p.strip() for p in v.split(",")]
    return [p for p in parts if p]

WORKDAY_DEBUG   = _env_flag("WORKDAY_DEBUG", False)
WD_MAX_HOSTS    = _env_int("WD_MAX_HOSTS",  4)
WD_MAX_SITES    = _env_int("WD_MAX_SITES",  8)
WD_MAX_PAGES    = _env_int("WD_MAX_PAGES",  1)
WD_EARLY_BREAK  = _env_flag("WD_EARLY_BREAK", True)
WD_LOCALES      = _env_list("WD_LOCALES", ["none","en-US","en_GB","locale:en_US","locale:en-GB"])
LIMIT           = 200
TIMEOUT_SECS    = 18.0
PAUSE_BETWEEN   = 0.6  # small politeness delay to avoid 429s

DEFAULT_SITES = [
    "External","Global","Careers","Jobs","Job","JobBoard",
    # common real site ids
    "UMGUS","UMGUK","universal-music-group","UNIVERSAL-MUSIC-GROUP",
    "WMGUS","WMGGLOBAL","WMG","Wmg",
]

# A more complete, browser-like header set. These matter on many Workday edges.
def _host_headers(host: str, site: str) -> Dict[str,str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Origin": f"https://{host}",
        "Referer": f"https://{host}/{site}",
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        # These increase acceptance on some tenants:
        "X-Requested-With": "XMLHttpRequest",
        "X-Workday-Client": "browser",     # harmless hint used by UI
        "X-Workday-Request-Id": str(uuid.uuid4()),
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        # Sec-CH hints – some edges branch on these:
        "Sec-CH-UA": '"Chromium";v="124", "Not:A-Brand";v="99"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Linux"',
    }

class HttpRetriableError(Exception): pass

def _locale_params(token: str) -> Dict[str, Any]:
    if token == "none":
        return {"activeOnly": "true"}
    if token.startswith("locale:"):
        return {"activeOnly": "true", "locale": token.split(":",1)[1]}
    if token in ("en-US","en_GB"):
        return {"activeOnly": "true", "lang": token}
    return {"activeOnly": "true"}

def _norm(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, (int,float)): return str(x)
    return str(x).strip()

def _join(parts: List[str], sep=", ") -> str:
    return sep.join([p for p in parts if p])

def _parse_items(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict): return []
    for k in ("jobPostings","items","value","postings","data"):
        v = data.get(k)
        if isinstance(v, list) and v:
            return v
    body = data.get("body") or data.get("result")
    if isinstance(body, dict):
        for k in ("jobPostings","items","value","postings","data"):
            v = body.get(k)
            if isinstance(v, list) and v:
                return v
    return []

def _extract_id(obj: Dict[str, Any]) -> str:
    for k in ("id","jobPostingId","requisitionId","jobReqId","number"):
        v = _norm(obj.get(k))
        if v: return v
    return ""

def _extract_title(obj: Dict[str, Any]) -> str:
    for k in ("title","jobPostingTitle","name","displayTitle","requisitionTitle"):
        v = _norm(obj.get(k))
        if v: return v
    return ""

def _extract_url(host: str, obj: Dict[str, Any]) -> str:
    for k in ("externalUrl","applyUrl","url","externalPath"):
        v = _norm(obj.get(k))
        if v:
            if v.startswith("/"):
                return f"https://{host}{v}"
            return v
    links = obj.get("links")
    if isinstance(links, list):
        for ln in links:
            if isinstance(ln, dict):
                href = _norm(ln.get("href"))
                if href:
                    if href.startswith("/"):
                        return f"https://{host}{href}"
                    return href
    return ""

def _extract_snippet(obj: Dict[str, Any]) -> str:
    for k in ("jobPostingDescription","jobDescription","description","summary"):
        v = obj.get(k)
        if isinstance(v, str): return v[:240]
        if isinstance(v, dict):
            t = v.get("text") or v.get("html") or v.get("value")
            if isinstance(t, str): return t[:240]
    return ""

def _extract_location(obj: Dict[str, Any]) -> str:
    locs = obj.get("locations")
    if isinstance(locs, list) and locs:
        out = []
        for l in locs:
            if not isinstance(l, dict): continue
            city = _norm(l.get("city") or l.get("cityName") or l.get("cityText"))
            region = _norm(l.get("region") or l.get("state") or l.get("province"))
            country = _norm(l.get("country") or l.get("countryName") or l.get("countryCode"))
            out.append(_join([city, region, country]))
        if out:
            return "; ".join([p for p in out if p])

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

    for k in ("locationText","city","state","country","countryCode"):
        v = _norm(obj.get(k))
        if v: return v
    return ""

@retry(wait=wait_exponential(min=1, max=8), stop=stop_after_attempt(3),
       retry=retry_if_exception_type(HttpRetriableError))
def _get(client: httpx.Client, url: str, params: Dict[str, Any], headers: Dict[str,str]) -> httpx.Response:
    r = client.get(url, params=params, headers=headers)
    # Workday rate-limits hard (429) and sometimes replies 5xx on bad cookie/header mixes.
    if r.status_code == 429 or (500 <= r.status_code < 600):
        raise HttpRetriableError(f"{r.status_code} for {url}")
    return r

def _site_candidates(company: Dict[str, Any]) -> List[str]:
    sites: List[str] = []
    user_sites = company.get("workday_sites") or company.get("sites")
    if isinstance(user_sites, list) and user_sites:
        for s in user_sites:
            if isinstance(s, str) and s.strip():
                sites.append(s.strip())

    # Expand if UMG/WMG by name
    name = (company.get("name") or "").lower()
    if "universal" in name or "umg" in name:
        for s in ("UMGUS","UMGUK","universal-music-group","UNIVERSAL-MUSIC-GROUP"):
            if s not in sites: sites.append(s)
    if "warner" in name or "wmg" in name:
        for s in ("WMGUS","WMGGLOBAL","WMG","Wmg"):
            if s not in sites: sites.append(s)

    for s in DEFAULT_SITES:
        if s not in sites:
            sites.append(s)
    return sites[:WD_MAX_SITES]

def _hosts(company: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    hosts = company.get("workday_hosts")
    if isinstance(hosts, list) and hosts:
        out.extend([h.strip() for h in hosts if isinstance(h, str) and h.strip()])

    host = company.get("workday_host")
    if isinstance(host, str) and host.strip() and host.strip() not in out:
        out.append(host.strip())

    tenant = (company.get("workday_tenant") or "").strip().lower()
    if tenant:
        # Common shards
        for shard in ("wd1","wd2","wd3","wd5","wd6"):
            guess = f"{tenant}.{shard}.myworkdayjobs.com"
            if guess not in out:
                out.append(guess)

    if not out:
        out.append("myworkdayjobs.com")
    return out[:WD_MAX_HOSTS]

def _paths(tenant: str, site: str) -> List[str]:
    return [f"/wday/cxs/{tenant}/{site}/jobs", f"/wday/cxs/{tenant}/jobs"]

def _warmup(client: httpx.Client, host: str, site: str):
    # Warm cookies on the exact host + site context
    warm = [
        f"https://{host}/{site}",
        f"https://{host}/wday/authenticate",
        f"https://{host}/",
    ]
    for u in warm:
        try:
            client.get(u, headers={"User-Agent": _host_headers(host, site)["User-Agent"]}, follow_redirects=True)
        except Exception:
            pass
        time.sleep(0.2)

def _map_item(host: str, company: Dict[str, Any], itm: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source": "workday",
        "company": company.get("name"),
        "id": _extract_id(itm) or None,
        "title": _extract_title(itm),
        "location": _extract_location(itm),
        "remote": False,
        "department": _norm(itm.get("department") or itm.get("jobFamily") or itm.get("category")),
        "team": None,
        "url": _extract_url(host, itm) or None,
        "posted_at": _norm(itm.get("postedOn") or itm.get("startDate") or itm.get("postedDate")) or None,
        "description_snippet": _extract_snippet(itm),
    }

def fetch(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    if (company.get("ats") or "").lower() != "workday":
        return []

    attempts: List[Dict[str, Any]] = []
    out: List[Dict[str, Any]] = []

    tenant = (company.get("workday_tenant") or "").strip()
    if not tenant:
        return []

    hosts = _hosts(company)
    sites  = _site_candidates(company)

    # Session per company
    with httpx.Client(timeout=TIMEOUT_SECS, follow_redirects=True) as client:
        for host in hosts:
            # Scope cookie to exact host (helps some edges)
            try:
                client.cookies.set("wd-browser-id", str(uuid.uuid4()), domain=host)
            except Exception:
                client.cookies.set("wd-browser-id", str(uuid.uuid4()))

            for site in sites:
                _warmup(client, host, site)
                hh = _host_headers(host, site)

                for path in _paths(tenant, site):
                    offset = 0
                    pages_done = 0
                    while pages_done < WD_MAX_PAGES:
                        got_this_page = 0

                        for tok in WD_LOCALES:
                            params = {"limit": LIMIT, "offset": offset}
                            params.update(_locale_params(tok))
                            url = f"https://{host}{path}"

                            try:
                                r = _get(client, url, params, hh)
                            except HttpRetriableError as _:
                                attempts.append({"u": url, "p": params, "s": "5xx/429", "items": 0})
                                continue
                            except Exception as e:
                                attempts.append({"u": url, "p": params, "s": "error", "err": str(e), "items": 0})
                                continue

                            ct = (r.headers.get("Content-Type") or "").lower()
                            status = r.status_code

                            if status != 200 or "json" not in ct:
                                # Log the real status to understand blocks vs empties
                                attempts.append({"u": url, "p": params, "s": status, "ct": ct, "items": 0})
                                continue

                            try:
                                data = r.json()
                            except Exception as e:
                                attempts.append({"u": url, "p": params, "s": "bad-json", "err": str(e), "items": 0})
                                continue

                            items = _parse_items(data)
                            if not items:
                                attempts.append({"u": url, "p": params, "s": status, "ct": ct, "items": 0})
                                continue

                            mapped = [_map_item(host, company, itm) for itm in items if isinstance(itm, dict)]
                            out.extend(mapped)
                            got_this_page = len(mapped)
                            attempts.append({"u": url, "p": params, "s": status, "ct": ct, "items": got_this_page})

                            if WD_EARLY_BREAK and got_this_page > 0:
                                if WORKDAY_DEBUG:
                                    attempts_str = json.dumps(attempts)[:2000]
                                    print(f"WORKDAY_DEBUG {company.get('name')}: tried={attempts_str} got={len(out)}")
                                return out

                            break  # after a success attempt (or last failure) move on

                        if got_this_page == 0:
                            break  # try next PATH or SITE

                        pages_done += 1
                        if got_this_page < LIMIT:
                            break
                        offset += LIMIT

                        time.sleep(PAUSE_BETWEEN)

                time.sleep(PAUSE_BETWEEN)

    if WORKDAY_DEBUG:
        attempts_str = json.dumps(attempts)[:2000]
        print(f"WORKDAY_DEBUG {company.get('name')}: tried={attempts_str} got={len(out)}")

    return out
