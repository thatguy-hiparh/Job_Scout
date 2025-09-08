import datetime as dt
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    with httpx.Client(timeout=timeout, headers={"User-Agent":"job-scout/1.0"}) as c:
        return c.get(url)

def fetch(company):
    slug = company["slug"]
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    r = _get(url)
    r.raise_for_status()
    data = r.json()
    jobs=[]
    for x in data:
        cats = x.get("categories") or {}
        ts = x.get("createdAt")
        posted = None
        if ts:
            try: posted = dt.datetime.utcfromtimestamp(ts/1000).isoformat()
            except Exception: posted = None
        jobs.append({
            "source": "lever",
            "company": company["name"],
            "id": x.get("id"),
            "title": x.get("text"),
            "location": cats.get("location"),
            "remote": ("remote" in (cats.get("location") or "").lower()) or ("remote" in (x.get("text") or "").lower()),
            "department": cats.get("team"),
            "team": cats.get("department"),
            "url": x.get("hostedUrl") or x.get("applyUrl"),
            "posted_at": posted,
            "description_snippet": (x.get("lists") or [{}])[0].get("text","")[:240]
        })
    return jobs
