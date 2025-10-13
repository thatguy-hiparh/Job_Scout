vendors/filters.py — full replacement
filters_py = r'''# vendors/filters.py — full-file replacement
# Robust keyword + location filtering with EMEA/Remote prioritization and simple scoring.
# Interface preserved: filter_jobs(jobs: list[dict], kw: dict) -> list[dict]
# - jobs: list of normalized job dicts: {title, company, location, url, posted_at?}
# - kw (from config/keywords.yml): may contain keys
#     include: [str]          # required terms (any)
#     exclude: [str]          # drop if any
#     allow_unlocated: bool   # default True
#     location:
#         include: [str]      # allowed location substrings (any) — case-insensitive
#         exclude: [str]      # disallowed location substrings (any)
#     remote_terms: [str]     # optional override
#     emea_terms:   [str]     # optional override
# Returns: filtered jobs with a 'score' field, sorted desc.
from __future__ import annotations
from typing import List, Dict, Any

DEFAULT_REMOTE_TERMS = [
    "remote", "hybrid", "work from anywhere", "distributed", "remoto", "da remoto",
    "telelavoro", "home office", "work-from-home", "wfh"
]

# Broad EMEA hints; add specific countries to be explicit
DEFAULT_EMEA_TERMS = [
    "europe", "emea", "uk", "united kingdom", "ireland", "italy", "italia",
    "germany", "deutschland", "france", "spain", "portugal", "netherlands",
    "belgium", "austria", "sweden", "norway", "denmark", "finland",
    "switzerland", "poland", "czech", "slovakia", "romania", "bulgaria",
    "greece", "cyprus", "malta", "estonia", "latvia", "lithuania", "hungary",
    "slovenia", "croatia", "serbia", "bosnia", "montenegro", "albania",
    "macedonia", "moldova", "ukraine", "georgia", "armenia"
]

def _text(s: Any) -> str:
    return (s or "").strip()

def _has_any(text: str, needles: List[str]) -> bool:
    if not text or not needles: return False
    t = text.lower()
    return any((n or "").lower() in t for n in needles)

def _location_ok(job: Dict[str, Any], kw: Dict[str, Any]) -> bool:
    allow_unlocated = kw.get("allow_unlocated", True)
    loc_cfg = kw.get("location") or {}
    allow = loc_cfg.get("include") or kw.get("location_allowlist") or []  # backwards-compatible
    deny  = loc_cfg.get("exclude") or []
    loc = _text(job.get("location"))
    if not loc:
        return bool(allow_unlocated)
    if allow and not _has_any(loc, allow):
        return False
    if deny and _has_any(loc, deny):
        return False
    return True

def _score(job: Dict[str, Any], kw: Dict[str, Any]) -> float:
    title = _text(job.get("title"))
    company = _text(job.get("company"))
    location = _text(job.get("location"))
    hay = f"{title} | {company} | {location}"
    
    inc = kw.get("include") or []
    exc = kw.get("exclude") or []
    remote_terms = kw.get("remote_terms") or DEFAULT_REMOTE_TERMS
    emea_terms   = kw.get("emea_terms") or DEFAULT_EMEA_TERMS
    
    score = 0.0
    # positive: inclusion matches (each +1)
    for k in inc:
        if _has_any(hay, [k]): score += 1.0
    # negative: exclusion (hard drop handled before); add small penalty if somehow present
    for k in exc:
        if _has_any(hay, [k]): score -= 0.5
    
    # bonuses
    if _has_any(location, remote_terms): score += 2.0
    if _has_any(location, emea_terms):   score += 1.5
    
    # tiny boost for music-domain hints in title
    if _has_any(title, ["music", "audio", "catalog", "metadata", "royalties", "licensing"]):
        score += 0.5
    
    # recency hook: if adapter supplied ISO date in posted_at, give a nudge (not strictly required)
    if _text(job.get("posted_at")):
        score += 0.2
    
    return score

def filter_jobs(jobs: List[Dict[str, Any]], kw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Apply include/exclude keyword filter, location allow/deny, and add a score
    that prioritizes Remote + EMEA roles. Sorted by score descending.
    """
    if not isinstance(jobs, list):
        return []
    
    inc = kw.get("include") or []
    exc = kw.get("exclude") or []
    
    out: List[Dict[str, Any]] = []
    for j in jobs:
        title = _text(j.get("title"))
        company = _text(j.get("company"))
        location = _text(j.get("location"))
        hay = f"{title} | {company} | {location}"
        
        # include gate
        if inc and not _has_any(hay, inc):
            continue
        # exclude gate
        if exc and _has_any(hay, exc):
            continue
        # location gate
        if not _location_ok(j, kw):
            continue
        
        jj = dict(j)
        jj["score"] = _score(j, kw)
        out.append(jj)
    
    # Highest score first
    out.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return out
'''
