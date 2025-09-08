import datetime as dt
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    headers = {"User-Agent": "job-scout/1.0"}
    with httpx.Client(timeout=timeout, headers=headers) as c:
        return c.get(url)

def _build_url(slug, job):
    # Prefer direct URL if provided; else construct from shortcode
    for k in ("url", "application_url", "shortlink"):
        if job.get(k):
            return job[k]
    sc = job.get("shortcode") or job.get("id") or job.get("shortcode_id")
    if sc:
        return f"https://apply.workable.com/{slug}/j/{sc}/"
    return None

def _loc_str(loc):
    if not isinstance(loc, dict):
        return loc or ""
    parts = [loc.get("city"), loc.get("region"), loc.get("country")]
    return ", ".join([p for p in parts if p])

def fetch(company):
    """
    Public Workable endpoint (no auth):
    https://apply.workable.com/api/v3/accounts/<slug>/jobs
    """
    slug = company["slug"]
    base = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs?limit=200"
    r = _get(base)
    r.raise_for_status()
    data = r.json() or {}
    items = data.get("results") or data.get("jobs") or []
    jobs = []
    for x in items:
        title = x.get("title") or x.get("full_title")
        dept  = x.get("department") or (x.get("departments") or [{}])[0].get("name") if x.get("departments") else None
        loc   = _loc_str(x.get("location") or {})
        remote_flag = False
        # Workable often sets workplace/remote hints
        wp = (x.get("workplace") or "") if isinstance(x.get("workplace"), str) else ""
        remote_flag = "remote" in wp.lower() or "remote" in (loc or "").lower()
        url = _build_url(slug, x)
        ts  = x.get("published_at") or x.get("updated_at") or x.get("created_at")
        posted = None
        if ts:
            try:
                # ISO timestamps are typical; dt.fromisoformat may choke on Z suffix, so use fromtimestamp fallback if needed
                posted = dt.datetime.fromisoformat(ts.replace("Z","+00:00")).isoformat()
            except Exception:
                posted = ts
        jobs.append({
            "source": "workable",
            "company": company["name"],
            "id": x.get("shortcode") or x.get("id"),
            "title": title,
            "location": loc,
            "remote": remote_flag,
            "department": dept,
            "team": None,
            "url": url,
            "posted_at": posted,
            "description_snippet": (x.get("snippet") or x.get("summary") or "")[:240]
        })
    return jobs
