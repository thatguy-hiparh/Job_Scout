import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

BASE = "https://api.smartrecruiters.com/v1/companies/{slug}/postings"

HEADERS = {
    "User-Agent": "job-scout/1.0 (+https://github.com/thatguy-hiparh/Job_Scout)",
    "Accept": "application/json, text/plain, */*",
}

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, params=None, timeout=30):
    with httpx.Client(headers=HEADERS, timeout=timeout, follow_redirects=True) as c:
        return c.get(url, params=params or {})

def _to_loc(locobj):
    if not isinstance(locobj, dict):
        return ""
    parts = [locobj.get("city"), locobj.get("region"), locobj.get("country")]
    return ", ".join([p for p in parts if p])

def fetch(company):
    """
    SmartRecruiters public API
      GET /v1/companies/{slug}/postings?limit=&offset=
    Docs are semi-public; the endpoint is used by their career sites.
    """
    slug = company.get("slug") or company.get("name")
    if not slug:
        return []

    url = BASE.format(slug=slug.lower())
    limit = 100
    offset = 0
    results = []

    while True:
        try:
            r = _get(url, params={"limit": limit, "offset": offset})
            if r.status_code != 200 or "json" not in r.headers.get("Content-Type","").lower():
                break
            data = r.json() or {}
            items = data.get("content") or data.get("postings") or []
            if not isinstance(items, list) or not items:
                break

            for p in items:
                if not isinstance(p, dict):
                    continue
                pid   = p.get("id") or p.get("refNumber")
                title = p.get("name")
                loc   = _to_loc(p.get("location") or {})
                posted= p.get("releasedDate") or p.get("createdOn")
                url2  = p.get("applyUrl") or p.get("ref") or p.get("jobAdUrl") or p.get("jobUrl")

                snippet = ""
                ad = p.get("jobAd") or {}
                if isinstance(ad, dict):
                    sections = ad.get("sections") or []
                    if isinstance(sections, list) and sections:
                        # take first section text as snippet
                        first = sections[0] or {}
                        snippet = (first.get("text") or "")[:240]

                results.append({
                    "source": "smartrecruiters",
                    "company": company["name"],
                    "id": str(pid) if pid is not None else None,
                    "title": title,
                    "location": loc,
                    "remote": isinstance(loc, str) and ("remote" in loc.lower()),
                    "department": p.get("department") or None,
                    "team": None,
                    "url": url2,
                    "posted_at": posted,
                    "description_snippet": snippet,
                })
            # paginate
            if len(items) < limit:
                break
            offset += limit
        except Exception:
            break

    return results
