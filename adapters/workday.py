import httpx, json, os
from tenacity import retry, wait_exponential, stop_after_attempt

COMMON_TENANTS = ["wd1","wd2","wd3","wd5","wd103"]
COMMON_SITES = [
    "Careers","External","Global","Jobs","Job","JobBoard",
    "GLOBAL","US","USA","UK","EMEA","EU",
    "GlobalExternal","Global_External"
]
LOCALES = ["", "lang=en-US", "lang=en_GB", "locale=en_US", "locale=en-GB"]

DEFAULT_HEADERS = {
    "User-Agent": "job-scout/1.0 (+https://github.com/thatguy-hiparh/Job_Scout)",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://example.com/",
    "Origin": "https://example.com",
}

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    with httpx.Client(timeout=timeout, headers=DEFAULT_HEADERS, follow_redirects=True) as c:
        return c.get(url)

def _endpoints(company):
    slug  = (company.get("slug") or "").lower()
    host  = (company.get("workday_host") or slug).lower()
    tenant = (company.get("workday_tenant") or "")
    sites  = company.get("workday_sites")

    tenants = [tenant.lower()] if tenant else COMMON_TENANTS

    try_sites = []
    if isinstance(sites, list) and sites:
        try_sites.extend(sites)
    for v in {slug, slug.upper(), slug.capitalize()}:
        if v and v not in try_sites:
            try_sites.append(v)
    for s in COMMON_SITES:
        if s not in try_sites:
            try_sites.append(s)

    cands = []
    for t in tenants:
        for site in try_sites:
            base = f"https://{host}.{t}.myworkdayjobs.com/wday/cxs/{host}/{site}/jobs"
            cands.append(base)
            cands.append(f"{base}?limit=200")
            for q in LOCALES:
                if q:
                    cands.append(f"{base}?{q}")
    for site in try_sites:
        base = f"https://workdayjobs.com/wday/cxs/{host}/{site}/jobs"
        cands.append(base)
        cands.append(f"{base}?limit=200")
    # de-dup
    seen=set(); ordered=[]
    for u in cands:
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
            attempts.append({"u": url[:120], "s": r.status_code, "j": ("json" in ct)})
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
                break
        except Exception:
            continue

    if debug:
        print(f"WORKDAY_DEBUG {company['name']}: tried={json.dumps(attempts)[:2000]} got={len(jobs)}")

    return jobs
