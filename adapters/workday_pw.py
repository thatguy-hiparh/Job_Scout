import os
from typing import Dict, List, Any

from playwright.sync_api import sync_playwright

TIMEOUT_MS = 30000
PW_HEADLESS = (os.getenv("PW_HEADLESS", "1").strip().lower() in ("1","true","yes","on"))
LIMIT = int(os.getenv("WD_LIMIT", "200"))

def _norm(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, (int, float)): return str(x)
    return str(x).strip()

def _join(parts: List[str], sep=", ") -> str:
    return sep.join([p for p in parts if p])

def _sites(company: Dict[str, Any]) -> List[str]:
    sites: List[str] = []
    cfg = company.get("workday_sites")
    if isinstance(cfg, list) and cfg:
        sites.extend([s for s in cfg if isinstance(s, str) and s.strip()])

    name = (company.get("name") or "").lower()
    if "universal" in name or "umg" in name:
        for s in ("UMGUS","UMGUK","universal-music-group","UNIVERSAL-MUSIC-GROUP","External","Global"):
            if s not in sites: sites.append(s)
    if "warner" in name or "wmg" in name:
        for s in ("WMGUS","WMGGLOBAL","WMG","Wmg","External","Global"):
            if s not in sites: sites.append(s)

    # Always try some generic portals last
    for s in ("External","Global","Careers","Jobs","Job","JobBoard"):
        if s not in sites: sites.append(s)

    max_sites = int(os.getenv("WD_MAX_SITES", "6"))
    return sites[:max_sites]

def _hosts(company: Dict[str, Any]) -> List[str]:
    """
    Honor explicit hosts if provided; only guess shards if no explicit hosts exist.
    """
    explicit: List[str] = []
    h = company.get("workday_host")
    if isinstance(h, str) and h.strip():
        explicit.append(h.strip())
    hs = company.get("workday_hosts")
    if isinstance(hs, list) and hs:
        for it in hs:
            if isinstance(it, str) and it.strip() and it.strip() not in explicit:
                explicit.append(it.strip())

    if explicit:
        # If user provided hosts, ONLY use those.
        max_hosts = int(os.getenv("WD_MAX_HOSTS", "3"))
        return explicit[:max_hosts]

    # Otherwise, guess from tenant.
    out: List[str] = []
    tenant = (company.get("workday_tenant") or "").strip().lower()
    if tenant:
        for shard in ("wd1","wd2","wd3","wd5","wd6"):
            guess = f"{tenant}.{shard}.myworkdayjobs.com"
            if guess not in out:
                out.append(guess)
    if not out:
        out.append("myworkdayjobs.com")

    max_hosts = int(os.getenv("WD_MAX_HOSTS", "3"))
    return out[:max_hosts]

def _map_node(host: str, company: Dict[str, Any], node: Dict[str, Any]) -> Dict[str, Any]:
    locs = node.get("locations") or []
    loc_out: List[str] = []
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
        "source": "workday_pw",
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

def _extract_from_graphql_response(resp_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = (resp_json or {}).get("data") or {}
    jp = data.get("jobPostings") or {}
    edges = jp.get("edges") or []
    nodes = []
    for e in edges:
        if isinstance(e, dict) and isinstance(e.get("node"), dict):
            nodes.append(e["node"])
    return nodes

def fetch(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    if (company.get("ats") or "").lower() not in ("workday_pw","workday-playwright","workday-pw"):
        return []

    tenant = (company.get("workday_tenant") or "").strip()
    if not tenant:
        return []

    hosts = _hosts(company)
    sites = _sites(company)

    all_items: List[Dict[str, Any]] = []
    early_break = (os.getenv("WD_EARLY_BREAK","1").strip().lower() in ("1","true","yes","on"))

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=PW_HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        try:
            for host in hosts:
                for site in sites:
                    base = f"https://{host}/{site}"
                    items_here: List[Dict[str, Any]] = []

                    def handle_response(resp):
                        try:
                            url = resp.url
                            if "/graphql" in url and f"/{tenant}/" in url:
                                if resp.request.method == "POST" and resp.status == 200:
                                    ct = (resp.headers.get("content-type") or "").lower()
                                    if "json" in ct:
                                        data = resp.json()
                                        nodes = _extract_from_graphql_response(data)
                                        if nodes:
                                            items_here.extend(nodes)
                        except Exception:
                            pass

                    page.on("response", handle_response)

                    page.goto(base, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
                    page.wait_for_timeout(2500)  # let XHRs settle

                    if not items_here:
                        try:
                            page.keyboard.type(" ")
                            page.wait_for_timeout(1200)
                        except Exception:
                            pass

                    if items_here:
                        mapped = [_map_node(host, company, n) for n in items_here]
                        all_items.extend(mapped)
                        if early_break:
                            return all_items

        finally:
            try: context.close()
            except Exception: pass
            try: browser.close()
            except Exception: pass

    return all_items
