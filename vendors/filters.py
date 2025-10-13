# vendors/filters.py
import re

def _has_any(text: str, needles: list[str]) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(n.lower() in t for n in needles)

def _location_ok(job: dict, kw: dict) -> bool:
    allow_unlocated = kw.get("allow_unlocated", True)
    loc_allow = kw.get("location_allowlist") or []
    loc = (job.get("location") or "").strip()
    if not loc:
        return allow_unlocated
    if not loc_allow:
        return True
    return _has_any(loc, loc_allow)

def _rss_extra_drop(job: dict, kw: dict) -> bool:
    if (job.get("source") or "").lower() != "rss":
        return False
    deny = kw.get("rss_exclude_words") or []
    hay = f"{job.get('title','')} {job.get('description','')} {job.get('url','')}"
    return _has_any(hay, deny)

def filter_jobs(jobs: list[dict], kw: dict) -> list[dict]:
    inc = kw.get("include_keywords") or []
    exc = kw.get("exclude_keywords") or []

    out = []
    for j in jobs:
        text = f"{j.get('title','')} {j.get('description','')}"
        # include
        if inc and not _has_any(text, inc):
            continue
        # exclude
        if exc and _has_any(text, exc):
            continue
        # location focus
        if not _location_ok(j, kw):
            continue
        # extra RSS drops
        if _rss_extra_drop(j, kw):
            continue

        out.append(j)
    return out
