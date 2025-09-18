import os
import json
import uuid
import time
from typing import Dict, List, Any
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

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

GQL_DEBUG       = _env_flag("WORKDAY_GQL_DEBUG", False)
WD_MAX_HOSTS    = _env_int("WD_MAX_HOSTS",   3)
WD_MAX_SITES    = _env_int("WD_MAX_SITES",   6)
WD_MAX_PAGES    = _env_int("WD_MAX_PAGES",   2)
WD_EARLY_BREAK  = _env_flag("WD_EARLY_BREAK", True)
LIMIT           = _env_int("WD_LIMIT",  200)
TIMEOUT_SECS    = 20.0
PAUSE_BETWEEN   = 0.5

DEFAULT_SITES = [
    "External","Global","Careers","Jobs","Job","JobBoard",
    "UMGUS","UMGUK","universal-music-group","UNIVERSAL-MUSIC-GROUP",
    "WMGUS","WMGGLOBAL","WMG","Wmg",
]

# Workday public job board GraphQL payload (as used by myworkdayjobs UI)
GQL_QUERY = """
query SearchJobs($limit: Int!, $offset: Int!, $appliedFacets: AppliedFacetsInput, $searchText: String) {
  jobPostings(limit: $limit, offset: $offset, appliedFacets: $appliedFacets, searchText: $searchText) {
    totalCount
    edges {
      node {
        id
        title
        externalUrl
        applyUrl
        locations {
          city
          region
          country
          countryCode
        }
        postedOn
        jobPostingDescription
        jobFamily
        department
        category
      }
    }
  }
}
""".strip()

def _norm(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, (int,float)): return str(x)
    return str(x).strip()

def _join(parts: List[str], sep=", ") -> str:
    return sep.join([p for p in parts if p])

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
        for shard in ("wd1","wd2","wd3","wd5","wd6"):
            guess = f"{tenant}.{shard}.myworkdayjobs.com"
            if guess not in out:
                out.append(guess)
    if not out:
        out.append("myworkdayjobs.com")
    return out[:WD_MAX_HOSTS]

def _sites(company: Dict[str, Any]) -> List[str]:
    sites: List[str] = []
    user = company.get("workday_sites") or company.get("sites")
    if isinstance(user, list) and user:
        sites.extend([s for s in user if isinstance(s, str) and s.strip()])
    name = (company.get("name") or "").lower()
    if "universal" in name or "umg" in name:
        for s in ("UMGUS","UMGUK","universal-music-group","UNIVERSAL-MUSIC-GROUP"):
            if s not in sites: sites.append(s)
    if "warner" in name or "wmg" in name:
        for s in ("WMGUS","WMGGLOBAL","WMG","Wmg"):
            if s not in sites: sites.append(s)
    for s in DEFAULT_SITES:
        if s not in sites: sites.append(s)
    return sites[:WD_MAX_SITES]

def _headers(host: str, site: str) -> Dict[str,str]:
    return {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Content-Type": "application/json",
        "Origin": f"https://{host}",
        "Referer": f"https://{host}/{site}",
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        "X-Requested-With": "XMLHttpRequest",
        "X-Workday-Client": "browser",
        "X-Workday-Request-Id": str(uuid.uuid4()),
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Sec-CH-UA": '"Chromium";v="124", "Not:A-Brand";v="99"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Linux"',
    }

def _warmup(client: httpx.Client, host: str, site: str):
    for u in (f"https://{host}/{site}",
              f"https://{host}/wday/authenticate",
              f"https://{host}/"):
        try:
            client.get(u, headers={"User-Agent": _headers(host, site)["User-Agent"]}, follow_redirects=True)
        except Exception:
            pass
        time.sleep(0.2)

def _map_node(host: str, company: Dict[str, Any], node: Dict[str, Any]) -> Dict[str, Any]:
    locs = node.get("locations") or []
    loc_out = []
    if isinstance(locs, list):
        for l in locs:
            if isinstance(l, dict):
                city = _norm(l.get("city"))
                region = _norm(l.get("region"))
                country = _norm(l.get("country") or l.get("countryCode"))
                loc_out.append(_join([city, region, country]))
    location = "; ".join([p for p in loc_out if p]) if loc_out else ""

    url = _norm(node.get("externalUrl") or node.get("applyUrl") or "")
    if url.startswith("/"):
        url = f"https://{host}{url}"

    return {
        "source": "workday_gql",
        "company": company.get("name"),
        "id": _norm(node.get("id")) or None,
        "title": _norm(node.get("title")),
        "location": location,
        "remote": False,
        "department": _norm(node.get("department") or node.get("jobFamily") or node.get("category")),
        "team": None,
        "url": url or None,
        "posted_at": _norm(node.get("postedOn")) or None,
        "description_snippet": _norm(node.get("jobPostingDescription"))[:240],
    }

class HttpRetriableError(Exception): pass

@retry(wait=wait_exponential(min=1, max=8), stop=stop_after_attempt(3),
       retry=retry_if_exception_type(HttpRetriableError))
def _post(client: httpx.Client, url: str, payload: Dict[str, Any], headers: Dict[str,str]) -> httpx.Response:
    r = client.post(url, json=payload, headers=headers)
    if r.status_code == 429 or (500 <= r.status_code < 600):
        raise HttpRetriableError(f"{r.status_code} for {url}")
    return r

def fetch(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    # only handle workday_gql companies
    if (company.get("ats") or "").lower() != "workday_gql":
        return []

    out: List[Dict[str, Any]] = []
    attempts: List[Dict[str, Any]] = []

    tenant = (company.get("workday_tenant") or "").strip()
    if not tenant:
        return []

    hosts = _hosts(company)
    sites = _sites(company)

    with httpx.Client(timeout=TIMEOUT_SECS, follow_redirects=True) as client:
        for host in hosts:
            try:
                client.cookies.set("wd-browser-id", str(uuid.uuid4()), domain=host)
            except Exception:
                client.cookies.set("wd-browser-id", str(uuid.uuid4()))

            for site in sites:
                _warmup(client, host, site)
                hh = _headers(host, site)
                gql_url = f"https://{host}/wday/cxs/{tenant}/{site}/graphql"

                offset = 0
                pages = 0
                while pages < WD_MAX_PAGES:
                    payload = {
                        "operationName": "SearchJobs",
                        "variables": {
                            "limit": LIMIT,
                            "offset": offset,
                            "searchText": None,
                            "appliedFacets": {}  # fetch everything, we'll filter later
                        },
                        "query": GQL_QUERY
                    }

                    try:
                        resp = _post(client, gql_url, payload, hh)
                    except HttpRetriableError:
                        attempts.append({"u": gql_url, "offset": offset, "s": "5xx/429", "items": 0})
                        break
                    except Exception as e:
                        attempts.append({"u": gql_url, "offset": offset, "s": "error", "err": str(e), "items": 0})
                        break

                    ct = (resp.headers.get("Content-Type") or "").lower()
                    if resp.status_code != 200 or "json" not in ct:
                        attempts.append({"u": gql_url, "offset": offset, "s": resp.status_code, "ct": ct, "items": 0})
                        break

                    try:
                        data = resp.json()
                    except Exception as e:
                        attempts.append({"u": gql_url, "offset": offset, "s": "bad-json", "err": str(e), "items": 0})
                        break

                    jp = (((data or {}).get("data") or {}).get("jobPostings") or {})
                    edges = jp.get("edges") or []
                    if not isinstance(edges, list) or not edges:
                        attempts.append({"u": gql_url, "offset": offset, "s": resp.status_code, "ct": ct, "items": 0})
                        break

                    mapped = []
                    for e in edges:
                        if isinstance(e, dict) and isinstance(e.get("node"), dict):
                            mapped.append(_map_node(host, company, e["node"]))

                    out.extend(mapped)
                    attempts.append({"u": gql_url, "offset": offset, "s": resp.status_code, "ct": ct, "items": len(mapped)})

                    if WD_EARLY_BREAK and mapped:
                        if GQL_DEBUG:
                            print(f"WORKDAY_GQL_DEBUG {company.get('name')}: tried={json.dumps(attempts)[:2000]} got={len(out)}")
                        return out

                    if len(mapped) < LIMIT:
                        break

                    offset += LIMIT
                    pages += 1
                    time.sleep(PAUSE_BETWEEN)

                time.sleep(PAUSE_BETWEEN)

    if GQL_DEBUG:
        print(f"WORKDAY_GQL_DEBUG {company.get('name')}: tried={json.dumps(attempts)[:2000]} got={len(out)}")

    return out
