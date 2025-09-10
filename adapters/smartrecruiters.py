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

def _country(locobj):
    if isinstance(locobj, dict):
        return (locobj.get("country") or "").strip()
    return ""

def _city(locobj):
    if isinstance(locobj, dict):
        return (locobj.get("city") or "").strip()
    return ""

def _iter_company_slugs(company):
    """
    Support either:
      slug: "randstad"
    OR
      smartrecruiters_slugs: ["adeccoitaly","adecco","adecco-it"]
    """
    slugs = []
    multi = company.get("smartrecruiters_slugs")
    if isinstance(multi, list) and multi:
        slugs.extend([s for s in multi if isinstance(s, str) and s.strip()])
    s = company.get("slug")
    if isinstance(s, str) and s.strip():
        if s not in slugs:
            slugs.append(s)
    # fall back to name
    if not slugs and company.get("name"):
        slugs.append(company["name"])
    return slugs

def fetch(company):
    """
    SmartRecruiters public API
      GET /v1/companies/{slug}/postings?limit=&offset=
    Adds optional company-scoped geo filters:
      smartrecruiters_countries: ["Italy","Ireland",...]
      smartrecruiters_cities: ["Milan","Rome",...]
    """
    slugs = _iter_company_slugs(company)
    if not slugs:
        return []

    limit = 100
    results = []

    allowed_countries = set([c.lower() for c in company.get("smartrecruiters_countries", []) if isinstance(c, str)])
    allowed_cities    = set([c.lower() for c in company.get("smartrecruiters_cities", []) if isinstance(c, str)])

    def allow_by_geo(locobj):
        if not allowed_countries and not allowed_cities:
            return True
        ctry = _country(locobj).lower()
        city = _city(locobj).lower()
        ok = True
        if allowed_countries:
            ok = ok and (ctry in allowed_countries)
        if allowed_cities:
            ok = ok and any(x in city for x in allowed_cities)
        return ok

    for slug in slugs:
        url = BASE.format(slug=slug)
        offset = 0
        seen_ids = set()

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
                    if not allow_by_geo(p.get("location") or {}):
                        continue

                    pid   = p.get("id") or p.get("refNumber")
                    if pid in seen_ids:
                        continue
                    seen_ids.add(pid)

                    title = p.get("name")
                    loc   = _to_loc(p.get("location") or {})
                    posted= p.get("releasedDate") or p.get("createdOn")
                    url2  = p.get("applyUrl") or p.get("ref") or p.get("jobAdUrl") or p.get("jobUrl")

                    snippet = ""
                    ad = p.get("jobAd") or {}
                    if isinstance(ad, dict):
                        sections = ad.get("sections") or []
                        if isinstance(sections, list) and sections:
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
