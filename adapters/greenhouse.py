import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    with httpx.Client(timeout=timeout, headers={"User-Agent":"job-scout/1.0"}) as c:
        return c.get(url)

def fetch(company):
    slug = company["slug"]
    # Use Greenhouse public boards API (more robust than the embed JSON)
    jurl = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    r = _get(jurl)
    r.raise_for_status()
    data = r.json() or {}
    items = data.get("jobs", [])
    jobs=[]
    for x in items:
        location = (x.get("location") or {}).get("name", "")
        dep = None
        # Greenhouse v1 includes departments inside metadata sometimes; keep None if absent
        jobs.append({
            "source": "greenhouse",
            "company": company["name"],
            "id": x.get("id"),
            "title": x.get("title"),
            "location": location,
            "remote": "remote" in (location or "").lower(),
            "department": dep,
            "team": None,
            "url": x.get("absolute_url") or x.get("absolute_url"),
            "posted_at": x.get("updated_at") or x.get("created_at"),
            "description_snippet": "",
        })
    return jobs
