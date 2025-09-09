import httpx, time
from tenacity import retry, wait_exponential, stop_after_attempt

COMMON_TENANTS = ["wd1","wd2","wd3","wd5"]
# Site names used by many orgs; we try slug + these variants
COMMON_SITES   = ["Careers","careers","External","external","Global","global"]

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    with httpx.Client(timeout=timeout, headers={"User-Agent": "job-scout/1.0"}) as c:
        return c.get(url)

def _endpoints(slug):
    """
    Try a matrix of Workday 'cxs' endpoints.
    Patterns observed:
      https://<slug>.wdX.myworkdayjobs.com/wday/cxs/<slug>/<site>/jobs
    We'll try: site=<slug> and common site names.
    """
    # prefer lowercase in path components
    base_slug = slug.lower()
    candidates = []
    for tenant in COMMON_TENANTS:
        # try <slug>/<slug> first
        candidates.append(f"https://{base_slug}.{tenant}.myworkdayjobs.com/wday/cxs/{base_slug}/{base_slug}/jobs")
        # try common site names
        for site in COMMON_SITES:
            candidates.append(f"https://{base_slug}.{tenant}.myworkdayjobs.com/wday/cxs/{base_slug}/{site}/jobs")
    # Also try workdayjobs.com (some orgs migrate there)
    for site in [base_slug] + COMMON_SITES:
        candidates.append(f"https://workdayjobs.com/wday/cxs/{base_slug}/{site}/jobs")
    # de-dup while preserving order
    seen=set(); ordered=[]
    for u in candidates:
        if u not in seen:
            seen.add(u); ordered.append(u)
    return ordered

def fetch(company):
    slug = company["slug"]
    jobs=[]
    last_err=None
    for url in _endpoints(slug):
        try:
            r = _get(url)
            if r.status_code != 200:
                continue
            if "application/json" not in (r.headers.get("Content-Type","").lower()):
                continue
            data = r.json()
            # structure: {"jobPostings":[{...}], "total": N, ...}
            postings = data.get("jobPostings") or data.get("data") or []
            if not isinstance(postings, list):
                continue
            for p in postings:
                if not isinstance(p, dict):
                    continue
                jid   = p.get("bulletFields") or p.get("id") or p.get("jobPostingId") or p.get("title")
                title = p.get("title") or (p.get("titleText") or {}).get("text")
                loc   = p.get("locationsText") or (p.get("primaryLocation") or {}).get("name")
                url2  = p.get("externalPath") or p.get("externalUrl")
                if url2 and url2.startswith("/"):
                    # construct absolute
                    base = url.split("/wday/")[0]
                    url2 = base + url2
                # remote heuristics
                remote_flag = False
                for field in (loc, title):
                    if isinstance(field, str) and "remote" in field.lower():
                        remote_flag = True
                posted = p.get("postedOn") or p.get("startDate") or p.get("postedDate")
                snippet = (p.get("shortText") or p.get("description") or "")
                if isinstance(snippet, dict):
                    snippet = snippet.get("text","")
                jobs.append({
                    "source":"workday",
                    "company": company["name"],
                    "id": str(jid),
                    "title": title,
                    "location": loc,
                    "remote": remote_flag,
                    "department": p.get("jobFamily") or (p.get("primaryJobPostingCategory") or {}).get("name"),
                    "team": None,
                    "url": url2,
                    "posted_at": posted,
                    "description_snippet": (snippet or "")[:240],
                })
            # we consider first working endpoint sufficient
            if jobs:
                break
        except Exception as e:
            last_err = e
            continue
    # If nothing resolved, return empty list (don’t crash the run)
    return jobs
