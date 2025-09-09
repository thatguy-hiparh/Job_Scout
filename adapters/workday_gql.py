import os, json
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

# Some tenants require these headers to return JSON
HDRS = {
    "User-Agent": "job-scout/1.0 (+https://github.com/thatguy-hiparh/Job_Scout)",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://example.com",
    "Referer": "https://example.com/",
    "x-workday-locale": "en-US",
}

COMMON_TENANTS = ["wd1","wd2","wd3","wd5","wd103"]
COMMON_SITES   = [
    "Careers","External","Global","Jobs","Job","JobBoard",
    "GLOBAL","US","USA","UK","EMEA","EU",
    "GlobalExternal","Global_External"
]

# We try a few query shapes used across Workday portals.
# Adapter is shape-tolerant on response parsing.
Q1 = {
    "operationName": "JobSearch",
    "variables": {"keyword": "", "limit": 50, "offset": 0, "facets": [], "location": None},
    "query": """
query JobSearch($keyword: String, $limit: Int, $offset: Int, $facets: [FacetInput!], $location: String) {
  jobSearch(keyword: $keyword, limit: $limit, offset: $offset, facets: $facets, location: $location) {
    totalCount
    jobPostings {
      id
      title
      externalPath
      locationsText
      postedOn
      shortText
      primaryLocation { name }
      primaryJobPostingCategory { name }
    }
  }
}""",
}

Q2 = {
    "operationName": "JobSearch",
    "variables": {"query": "", "page": 1, "pageSize": 50, "facets": []},
    "query": """
query JobSearch($query: String, $page: Int, $pageSize: Int, $facets: [FacetInput!]) {
  jobSearch(query: $query, page: $page, pageSize: $pageSize, facets: $facets) {
    totalCount
    jobs {
      id
      title
      externalPath
      locationsText
      postedOn
      shortText
      primaryLocation { name }
      primaryJobPostingCategory { name }
    }
  }
}""",
}

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _post(url, payload, timeout=30):
    with httpx.Client(timeout=timeout, headers=HDRS, follow_redirects=True) as c:
        return c.post(url, content=json.dumps(payload))

def _endpoints(company):
    slug = (company.get("slug") or "").lower()
    host = (company.get("workday_host") or slug).lower()
    tenant = (company.get("workday_tenant") or "")
    sites = company.get("workday_sites")

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

    # Build graphql endpoints
    cand = []
    for t in tenants:
        for site in try_sites:
            cand.append(f"https://{host}.{t}.myworkdayjobs.com/wday/graphql/{host}/{site}")
    # de-dupe
    seen=set(); ordered=[]
    for u in cand:
        if u not in seen:
            seen.add(u); ordered.append(u)
    return ordered

def _parse_payload(obj):
    """Return list of postings from either {jobSearch:{jobPostings}} or {jobSearch:{jobs}}."""
    if not isinstance(obj, dict):
        return []
    data = obj.get("data") or {}
    js = data.get("jobSearch") or {}
    posts = js.get("jobPostings")
    if isinstance(posts, list) and posts:
        return posts
    posts = js.get("jobs")
    if isinstance(posts, list) and posts:
        return posts
    return []

def fetch(company):
    debug = os.getenv("DEBUG_WORKDAY","").strip().lower() in ("1","true","yes","on")
    attempts = []
    results = []

    for endpoint in _endpoints(company):
        # try both query shapes with pagination (offset or page)
        total = None
        offset = 0
        page = 1
        hit = False

        for shape in ("Q1","Q2"):
            # reset pagers for each query shape
            total = None
            offset = 0
            page = 1
            while True:
                payload = Q1 if shape == "Q1" else Q2
                pl = json.loads(json.dumps(payload))  # deep copy

                if shape == "Q1":
                    pl["variables"]["offset"] = offset
                else:
                    pl["variables"]["page"] = page

                try:
                    r = _post(endpoint, pl)
                    ok = (r.status_code == 200 and "json" in (r.headers.get("Content-Type","").lower()))
                    attempts.append({"u": endpoint[:120], "s": r.status_code, "ok": ok, "shape": shape, "page": page, "offset": offset})
                    if not ok:
                        break  # try the other shape / endpoint

                    payload_json = r.json()
                    postings = _parse_payload(payload_json)
                    if not postings:
                        break

                    # normalize
                    for p in postings:
                        if not isinstance(p, dict):
                            continue
                        jid   = p.get("id") or p.get("jobPostingId") or p.get("title")
                        title = p.get("title")
                        loc   = p.get("locationsText") or (p.get("primaryLocation") or {}).get("name")
                        url2  = p.get("externalPath")
                        if url2 and url2.startswith("/"):
                            base = endpoint.split("/wday/")[0]
                            url2 = base + url2
                        posted = p.get("postedOn")
                        dept   = (p.get("primaryJobPostingCategory") or {}).get("name")
                        desc   = p.get("shortText") or ""

                        results.append({
                            "source": "workday_gql",
                            "company": company["name"],
                            "id": str(jid) if jid is not None else None,
                            "title": title,
                            "location": loc,
                            "remote": isinstance(loc, str) and ("remote" in loc.lower()),
                            "department": dept,
                            "team": None,
                            "url": url2,
                            "posted_at": posted,
                            "description_snippet": (desc or "")[:240],
                        })

                    hit = True
                    # page forward
                    if shape == "Q1":
                        if len(postings) < pl["variables"]["limit"]:
                            break
                        offset += pl["variables"]["limit"]
                    else:
                        if len(postings) < pl["variables"]["pageSize"]:
                            break
                        page += 1

                except Exception:
                    break  # change shape / endpoint

            if hit:
                break  # no need to try other shapes for this endpoint

        if hit:
            break  # first endpoint that yields data is enough

    if debug:
        print(f"WORKDAY_GQL_DEBUG {company['name']}: tried={json.dumps(attempts)[:1800]} got={len(results)}")

    return results
