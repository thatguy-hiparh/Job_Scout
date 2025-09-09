import datetime as dt
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, timeout=30):
    headers = {"User-Agent": "job-scout/1.0"}
    with httpx.Client(timeout=timeout, headers=headers) as c:
        return c.get(url)

def _try_endpoints(slug):
    # Try both public patterns
    yield f"https://{slug}.ashbyhq.com/api/public/jobs"
    yield f"https://api.ashbyhq.com/posting-api/job-board/{slug}"

def _iso(ts):
    if not ts: return None
    s = str(ts)
    try:
        return dt.datetime.fromisoformat(s.replace("Z","+00:00")).isoformat()
    except Exception:
        return s

def _loc_to_string(loc):
    if not loc: return ""
    if isinstance(loc, str): return loc
    if isinstance(loc, dict):
        parts = [loc.get("city"), loc.get("region"), loc.get("country")]
        return ", ".join([p for p in parts if p])
    if isinstance(loc, list) and loc:
        return _loc_to_string(loc[0])
    return ""

def _iter_jobs(raw):
    """
    Normalize across Ashby variants safely.
    Accepts dict, list, or odd wrappers; yields only dict jobs.
    """
    if raw is None:
        return
    # If it's a string or anything not iterable as jobs -> nothing
    if isinstance(raw, str):
        return
    # List? yield dict items only
    if isinstance(raw, list):
        for j in raw:
            if isinstance(j, dict):
                yield j
        return
    # Dict?
    if isinstance(raw, dict):
        # Common keys
        for key in ("jobs", "data", "results", "list"):
            v = raw.get(key)
            if isinstance(v, list):
                for j in v:
                    if isinstance(j, dict):
                        yield j
                return
            if isinstance(v, dict):
                nodes = v.get("nodes")
                if isinstance(nodes, list):
                    for j in nodes:
                        if isinstance(j, dict):
                            yield j
                    return
        # Some orgs use nested { jobBoard: { jobs: [...] } }
        jb = raw.get("jobBoard") or raw.get("job_board")
        if isinstance(jb, dict):
            v = jb.get("jobs") or jb.get("data")
            if isinstance(v, list):
                for j in v:
                    if isinstance(j, dict):
                        yield j
                return
    # Anything else -> nothing
    return

def fetch(company):
    slug = company["slug"]
    data = None; last_err = None
    for url in _try_endpoints(slug):
        try:
            r = _get(url); r.raise_for_status()
            # Some Ashby installs return text/html on error; guard here
            ct = r.headers.get("Content-Type","").lower()
            if "json" not in ct:
                # Not JSON – bail gracefully
                return []
            data = r.json()
            break
        except Exception as e:
            last_err = e
            continue
    if data is None:
        # Could be blocked or no public board – return empty, don't crash
        return []

    results = []
    for j in _iter_jobs(data) or []:
        # Ultra-defensive: only dicts
        if not isinstance(j, dict):
            continue

        jid = j.get("id") or j.get("jobId") or j.get("slug") or j.get("externalId")
        title = j.get("title") or (j.get("job") or {}).get("title")
        url   = j.get("jobUrl") or j.get("url") or j.get("applyUrl") or (j.get("job") or {}).get("url")
        dept  = j.get("departmentName") or (j.get("department") or {}).get("name")

        loc   = j.get("location") or j.get("jobLocations") or j.get("locations") or {}
        loc_str = _loc_to_string(loc)
        remote_flag = False
        if isinstance(loc, dict):
            remote_flag = bool(loc.get("remote"))
        elif isinstance(loc, list):
            remote_flag = any(isinstance(x, dict) and bool(x.get("remote")) for x in loc)
        elif isinstance(loc, str):
            remote_flag = "remote" in loc.lower()

        posted = _iso(j.get("publishedAt") or j.get("createdAt") or j.get("updatedAt"))

        desc = j.get("shortDescription") or j.get("description") or (j.get("job") or {}).get("description") or ""
        if isinstance(desc, dict) and "text" in desc:
            desc = desc["text"]

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
            "description_snippet": str(desc)[:240],
        })
    return results
