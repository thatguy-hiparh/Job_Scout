import datetime as dt
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    headers = {"User-Agent": "job-scout/1.0"}
    with httpx.Client(timeout=timeout, headers=headers) as c:
        return c.get(url)

def _try_endpoints(slug):
    # Ashby has two common public endpoints; try both:
    # 1) Company subdomain
    yield f"https://{slug}.ashbyhq.com/api/public/jobs"
    # 2) Global posting API (some setups)
    yield f"https://api.ashbyhq.com/posting-api/job-board/{slug}"

def _loc_to_string(loc):
    if not loc:
        return ""
    # Ashby returns {'city':..., 'region':..., 'country':..., 'remote': bool}
    parts = []
    for k in ("city", "region", "country"):
        v = loc.get(k)
        if v: parts.append(v)
    return ", ".join(parts)

def fetch(company):
    slug = company["slug"]
    data = None
    last_err = None
    for url in _try_endpoints(slug):
        try:
            r = _get(url); r.raise_for_status()
            data = r.json()
            if data: break
        except Exception as e:
            last_err = e
            continue
    if data is None:
        raise RuntimeError(f"Ashby: no data for slug={slug}: {last_err}")

    # Normalize across both formats
    results = []
    jobs = data.get("jobs") or data.get("data") or []
    for j in jobs:
        # Fields vary slightly across endpoints
        jid = j.get("id") or j.get("jobId") or j.get("slug")
        title = j.get("title") or (j.get("job") or {}).get("title")
        url   = j.get("jobUrl") or j.get("url") or j.get("applyUrl")
        dept  = j.get("departmentName") or (j.get("department") or {}).get("name")
        loc   = j.get("location") or j.get("jobLocations") or {}
        # location can be dict or list of dicts
        if isinstance(loc, list) and loc:
            loc_str = _loc_to_string(loc[0])
            remote_flag = any(bool(x.get("remote")) for x in loc)
        else:
            loc_str = _loc_to_string(loc if isinstance(loc, dict) else {})
            remote_flag = bool((loc or {}).get("remote")) if isinstance(loc, dict) else False
        ts = j.get("publishedAt") or j.get("createdAt") or j.get("updatedAt")
        posted = None
        if ts:
            try:
                posted = dt.datetime.fromisoformat(str(ts).replace("Z","+00:00")).isoformat()
            except Exception:
                posted = str(ts)

        results.append({
            "source": "ashby",
            "company": company["name"],
            "id": jid,
            "title": title,
            "location": loc_str,
            "remote": remote_flag or ("remote" in (loc_str or "").lower()),
            "department": dept,
            "team": None,
            "url": url,
            "posted_at": posted,
            "description_snippet": (j.get("shortDescription") or j.get("description") or "")[:240],
        })
    return results
