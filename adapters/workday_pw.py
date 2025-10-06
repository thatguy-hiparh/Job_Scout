import os
from typing import Dict, List, Any
from playwright.sync_api import sync_playwright

TIMEOUT_MS = 30000
PW_HEADLESS = (os.getenv("PW_HEADLESS", "1").strip().lower() in ("1","true","yes","on"))
LIMIT = int(os.getenv("WD_LIMIT", "200"))
MAX_SITES = int(os.getenv("WD_MAX_SITES", "6"))
MAX_HOSTS = int(os.getenv("WD_MAX_HOSTS", "3"))
EARLY_BREAK = (os.getenv("WD_EARLY_BREAK","1").strip().lower() in ("1","true","yes","on"))

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

    # Generic fallbacks at the end
    for s in ("External","Global","Careers","Jobs","Job","JobBoard"):
        if s not in sites: sites.append(s)

    return sites[:MAX_SITES]

def _hosts(company: Dict[str, Any]) -> List[str]:
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
        return explicit[:MAX_HOSTS]

    # Guess from tenant only if no explicit hosts
    out: List[str] = []
    tenant = (company.get("workday_tenant") or "").strip().lower()
    if tenant:
        for shard in ("wd1","wd5","wd3","wd6"):  # avoid wd2 noise
            out.append(f"{tenant}.{shard}.myworkdayjobs.com")
    if not out:
        out.append("myworkdayjobs.com")
    return out[:MAX_HOSTS]

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
    ats = (company.get("ats") or "").lower()
    if ats not in ("workday_pw","workday-playwright","workday-pw"):
        return []

    tenant = (company.get("workday_tenant") or "").strip()
    if not tenant:
        print(f"WORKDAY_PW {company.get('name')}: missing workday_tenant")
        return []

    hosts = _hosts(company)
    sites = _sites(company)

    tried_logs: List[Dict[str, Any]] = []
    all_items: List[Dict[str, Any]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=PW_HEADLESS)
        context = browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
        page = context.new_page()

        def handle_response(resp):
            try:
                url = resp.url
                if f"/wday/cxs/{tenant}/graphql" in url and resp.request.method == "POST" and resp.status == 200:
                    ct = (resp.headers.get("content-type") or "").lower()
                    if "json" in ct:
                        data = resp.json()
                        nodes = _extract_from_graphql_response(data)
                        if nodes:
                            tried_logs[-1]["items"] += len(nodes)
                            all_items.extend([_map_node(tried_logs[-1]["host"], company, n) for n in nodes])
            except Exception:
                pass

        page.on("response", handle_response)

        try:
            for host in hosts:
                for site in sites:
                    base = f"https://{host}/wday/cxs/{tenant}/{site}"
                    record = {"host": host, "site": site, "url": base, "status": None, "items": 0}
                    tried_logs.append(record)
                    try:
                        page.goto(base, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
                        record["status"] = "ok"
                        # Let the app boot & fire GraphQL
                        page.wait_for_load_state("networkidle", timeout=TIMEOUT_MS)
                        page.wait_for_timeout(1500)
                        # nudge
                        try:
                            page.keyboard.down("End"); page.wait_for_timeout(600); page.keyboard.up("End")
                        except Exception:
                            pass
                    except Exception as e:
                        record["status"] = f"error: {e}"
                        continue

                    if EARLY_BREAK and record["items"] > 0:
                        break
                if EARLY_BREAK and any(r["items"] > 0 for r in tried_logs):
                    break
        finally:
            try: context.close()
            except Exception: pass
            try: browser.close()
            except Exception: pass

    dbg = os.getenv("WORKDAY_GQL_DEBUG") or os.getenv("WORKDAY_PW_DEBUG")
    if str(dbg).strip().lower() in ("1","true","yes","on"):
        print(f"WORKDAY_PW_DEBUG {company.get('name')}: tried={tried_logs} got={len(all_items)}")

    return all_items
