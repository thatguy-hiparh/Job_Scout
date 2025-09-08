import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    with httpx.Client(timeout=timeout, headers={"User-Agent":"job-scout/1.0"}) as c:
        return c.get(url)

def fetch(company):
    slug = company["slug"]
    jurl = f"https://boards.greenhouse.io/{slug}/embed/job_board.json"
    r = _get(jurl)
    r.raise_for_status()
    board = r.json()
    jobs=[]
    for dep in board.get("departments", []):
        for x in dep.get("jobs", []):
            loc_name = (x.get("location") or {}).get("name","")
            jobs.append({
                "source": "greenhouse",
                "company": company["name"],
                "id": x.get("id"),
                "title": x.get("title"),
                "location": loc_name,
                "remote": "remote" in loc_name.lower(),
                "department": dep.get("name"),
                "team": None,
                "url": x.get("absolute_url"),
                "posted_at": None,
                "description_snippet": ""
            })
    return jobs
