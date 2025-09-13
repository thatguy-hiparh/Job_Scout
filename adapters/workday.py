import os
import json
from typing import Dict, List, Any, Tuple
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Workday CxS JSON endpoint pattern:
#   https://{host}/wday/cxs/{tenant}/{site}/jobs?limit=200&offset=0
# Common hosts:
#   umusic.wd5.myworkdayjobs.com
#   wmg.wd1.myworkdayjobs.com

DEFAULT_HEADERS = {
    "User-Agent": "job-scout/1.0 (+https://github.com/thatguy-hiparh/Job_Scout)",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://myworkdayjobs.com",
    "Referer": "https://myworkdayjobs.com/",
}

# Fallback list of possible "site" segments that many tenants expose.
DEFAULT_SITES = [
    "External",
    "Global",
    "Careers",
    "Jobs",
    "Job",
    "JobBoard",
]

# Workday often uses a branded site segment (e.g., UMGUS/UMGUK/WMGGLOBAL).
# We'll try those from companies.yml first; if none succeed, we try DEFAULT_SITES.

# Locale param variants — we’ll try a few to avoid 500/locale errors.
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
    """One-shot GET with explicit 429/5xx raising for tenacity to retry."""
    r = client.get(url, params=params)
    # Consider 429 and 5xx retriable:
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
    """
    Workday CxS JSON can differ per tenant. We try a few shapes:
      - {"jobPostings":[{...}]}
      - {"items":[{...}]}
      - {"value":[{...}]}
    """
    if not isinstance(data, dict):
        return []
    for key in ("jobPostings", "items", "value", "postings", "data"):
        v = data.get(key)
        if isinstance(v, list) and v:
            return v
    # Some pages nest under "body" or "result"
    body = data.get("body") or data.get("result")
    if isinstance(body, dict):
        for key in ("jobPostings", "items", "value", "postings", "data"):
            v = body.get(key)
            if isinstance(v, list) and v:
                return v
    return []

def _extract_location(obj: Dict[str, Any]) -> str:
    """
    Try common Workday fields to build a human-readable location string.
    """
    # Many tenants provide "locations" as a list of objects with city/region/country
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

    # Some return a single "location" object/string:
    loc = obj.get("location") or obj.get("primaryLocation")
    if isinstance(loc, dict):
        city = _norm(loc.get("city") or loc.get("cityName"))
        region = _norm(loc.get("region") or loc.get("state"))
        country = _norm(loc.get("country") or loc.get("countryName") or loc.get("countryCode"))
        s = _join_nonempty([city, region, country])
        if s:
            return s
        # Sometimes "location" is a nested label
        label = _norm(loc.get("label") or loc.get("name") or loc.get("displayName"))
        if label:
            return label
    elif isinstance(loc, str):
        if loc.strip():
            return loc.strip()

    # Fall back to any hints:
    for k in ("locationText", "city", "state", "country", "countryCode"):
        v = _norm(obj.get(k))
        if v:
            return v
    return ""

def _extract_url(obj: Dict[str, Any]) -> str:
    """
    Try to find a public job URL.
    Common fields: externalPath, externalUrl, applyUrl, url.
    """
    for k in ("externalUrl", "applyUrl", "url"):
        v = _norm(obj.get(k))
        if v:
            return v
    # Some return "externalPath" which needs to be combined by the caller.
    v = _norm(obj.get("externalPath"))
    if v:
        return v
    # Nested links array
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
    # Sometimes embedded under "number"
    v = _norm(obj.get("number"))
    if v:
        return v
    return ""

def _extract_snippet(obj: Dict[str, Any]) -> str:
    # Try common description containers
    for k in ("jobPostingDescription", "jobDescription", "description", "summary"):
        v = obj.get(k)
        if isinstance(v, str):
            return v[:240]
        if isinstance(v, dict):
            # Some tenants wrap in {"text": "..."}
            t = v.get("text") or v.get("html") or v.get("value")
            if isinstance(t, str):
                return t[:240]
    return ""

def _build_base(host: str, tenant: str, site: str) -> str:
    return f"https://{host}/wday/cxs/{tenant}/{site}/jobs"

def _site_candidates(company: Dict[str, Any]) -> List[str]:
    sites = []
    # Use explicit list if provided
    user_sites = company.get("workday_sites") or company.get("sites")
    if isinstance(user_sites, list) and user_sites:
        sites.extend([s for s in user_sites if isinstance(s, str) and s.strip()])

    # Heuristics / known branded segments
    name = (company.get("name") or "").lower()
    if "universal" in name or "umg" in name:
        for s in ("UMGUS", "UMGUK", "universal-music-group", "UNIVERSAL-MUSIC-GROUP"):
            if s not in sites:
                sites.append(s)
    if "warner" in name or "wmg" in name:
        for s in ("WMGUS", "WMGGLOBAL", "WMG", "Wmg"):
            if s not in sites:
                sites.append(s)

    # Always add generic tail candidates
    for s in DEFAULT_SITES:
        if s not in sites:
            sites.append(s)
    return sites

def _host_for(company: Dict[str, Any]) -> str:
    # Allow override via companies.yml
    host = company.get("workday_host")
    if isinstance(host, str) and host.strip():
        return host.strip()
    # Fallback by tenant
    tenant = company.get("workday_tenant") or ""
    t = tenant.strip().lower()
    # Common tenant-host mappings:
    if t == "umusic":
        return "umusic.wd5.myworkdayjobs.com"
    if t == "wmg":
        return "wmg.wd1.myworkdayjobs.com"
    # Generic default (most WD tenants are on myworkdayjobs.com):
    return f"{t}.wd5.myworkdayjobs.com" if t else "myworkdayjobs.com"

def fetch(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch Workday postings for the given company dict.
    Expected company keys:
      - name
      - ats: "workday"
      - workday_tenant: e.g., "umusic"
      - workday_host: (optional) full host override
      - workday_sites: (optional) list[str]
    """
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
        found_any = False
        for site in sites:
            # We’ll page until fewer than LIMIT are returned.
            offset = 0
            got_this_site_total = 0
            while True:
                page_got = 0
                for params in LOCALE_PARAM_SETS:
                    p = {"limit": LIMIT, "offset": offset}
                    p.update(params)
                    url = _build_base(host, tenant, site)
                    try:
                        r = _get(client, url, p)
                    except HttpRetriableError as e:
                        # Record retriable failure
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

                    # Map items
                    mapped = []
                    for itm in items:
                        if not isinstance(itm, dict):
                            continue
                        jid = _extract_id(itm)
                        title = _extract_title(itm)
                        loc = _extract_location(itm)
                        url2 = _extract_url(itm)
                        # If only externalPath provided, build a canonical URL
                        if url2 and url2.startswith("/"):
                            # Normalize to the host we're hitting
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
                    got_this_site_total += page_got
                    attempts.append({"u": url, "p": p, "s": r.status_code, "items": page_got})

                    # If we got a full page, there may be more:
                    if page_got == LIMIT:
                        # try next page (same locale params sequence)
                        continue
                    # Otherwise, break locale tries and paging for this site:
                    break  # break locale loop

                if page_got < LIMIT:
                    break  # stop paging this site
                offset += LIMIT

            if got_this_site_total > 0:
                found_any = True
                # You can break here if you want the first working site only.
                # We'll continue to accumulate from other sites too (safe, de-dupe upstream).
                # break

    if debug:
        # Cap printed attempts to keep logs readable
        attempts_str = json.dumps(attempts)[:1800]
        print(f"WORKDAY_DEBUG {company.get('name')}: tried={attempts_str} got={len(out)}")

    return out
