import httpx, json
from tenacity import retry, wait_exponential, stop_after_attempt
import os

# Tenants we see most in the wild; add more if needed
COMMON_TENANTS = ["wd1","wd2","wd3","wd5","wd103"]

# Broad set of site names to probe (case sensitive on server, but most accept these)
COMMON_SITES = [
    "Careers","External","Global","Jobs","Job","JobBoard",
    "GLOBAL","US","USA","UK","EMEA","EU",
    "GlobalExternal","Global_External"
]

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    with httpx.Client(timeout=timeout, headers={"User-Agent": "job-scout/1.0"}) as c:
        return c.get(url)

def _endpoints(company):
    """
    Build candidate Workday CXS endpoints. Supports overrides:
      workday_host, workday_tenant, workday_sites (list)
    Fallbacks try a wide set of sites + tenants.
    """
    slug = (company.get("slug") or "").lower()
    host = (company.get("workday_host") or slug).lower()
    tenant = (company.get("workday_tenant") or "")
    sites = company.get("workday_sites")

    tenants = [tenant.lower()] if tenant else COMMON_TENANTS

    # site try-list: explicit -> slug variants -> common
    try_sites = []
    if isinstance(sites, list) and sites:
        try_sites.extend(sites)
    # add some reasonable slug variants (brand names often equal site)
    for v in {slug, slug.upper(), slug.capitalize()}:
        if v and v not in try_sites:
            try_sites.append(v)
    # add commons
    for s in COMMON_SITES:
        if s not in try_sites:
            try_sites.append(s)

    candidates = []
    for t in tenants:
        for site in try_sites:
            candidates.append(f"https://{host}.{t}.myworkdayjobs.com/wday/cxs/{host}/{site}/jobs")
            # a few servers need explicit limit to return data
            candidates.append(f"https://{host}.{t}.myworkdayjobs.com/wday/cxs/{host}/{site}/jobs?limit=200")

    # also try generic workday host (some orgs migrated there)
    for site in try_sites:
        candidates.append(f"https://workdayjobs.com/wday/cxs/{host}/{site}/jobs")

    # de-dupe keep order
    seen=set(); ordered=[]
    for u in candidates:
        if u not in seen:
            seen.add(u); ordered.append(u)
    return ordered

def fetch(company):
    debug = os.getenv("DEBUG_WORKDAY","").strip().lower() in ("1","true","yes","on")
    attempts = []
    jobs=[]
    for url in _endpoints(company):
        try:
            r = _get(url)
            ct = (r.headers.get("Content-Type","") or "").lower()
            ok = (r.status_code == 200 and "json" in ct)
            attempts.append({"url": url, "status": r.status_code, "json": ("json" in ct)})
            if not ok:
                continue
            data = r.json()
            postings = data.get("jobPostings") or data.get("data") or []
            if not isinstance(postings, list) or not postings:
                continue
            for p in postings:
                if not isinstance(p, dict):
                    continue
                jid   = p.get("id") or p.get("jobPostingId") or p.get("title")
                title = p.get("title") or (p.get("titleText") or {}).get("text")
                loc   = p.get("locationsText") or (p.get("primaryLocation") or {}).get("name")
                url2  = p.get("externalPath") or p.get("externalUrl")
                if url2 and url2.startswith("/"):
                    base = url.split("/wday/")[0]
                    url2 = base + url2
                # remote heuristic
                remote_flag = False
                for field in (loc, title):
                    if isinstance(field, str) and "remote" in field.lower():
                        remote_flag = True
                posted = p.get("postedOn") or p.get("startDate") or p.get("postedDate")
                snippet = p.get("shortText") or p.get("description") or ""
                if isinstance(snippet, dict):
                    snippet = snippet.get("text","")

                jobs.append({
                    "source":"workday",
                    "company": company["name"],
                    "id": str(jid) if jid is not None else None,
                    "title": title,
                    "location": loc,
                    "remote": remote_flag,
                    "department": p.get("jobFamily") or (p.get("primaryJobPostingCategory") or {}).get("name"),
                    "team": None,
                    "url": url2,
                    "posted_at": posted,
                    "description_snippet": (snippet or "")[:240],
                })
            if jobs:
                break   # first endpoint that returns data is enough
        except Exception:
            # just try next
            continue

    if debug:
        # Print a compact debug line to Actions logs
        tried = [{"u":a["url"][:120], "s":a["status"], "j":a["json"]} for a in attempts]
        print(f"WORKDAY_DEBUG {company['name']}: tried={json.dumps(tried)[:2000]} got={len(jobs)}")

    return jobs
