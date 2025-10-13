# vendors/filters.py — full replacement
from __future__ import annotations
from typing import List, Dict, Any

DEFAULT_REMOTE_TERMS = [
    "remote", "hybrid", "work from anywhere", "distributed", "remoto", "da remoto",
    "telelavoro", "home office", "work-from-home", "wfh"
]

DEFAULT_EMEA_TERMS = [
    "europe", "emea", "uk", "united kingdom", "ireland", "italy", "italia",
    "germany", "deutschland", "france", "spain", "portugal", "netherlands",
    "belgium", "austria", "sweden", "norway", "denmark", "finland",
    "switzerland", "poland", "czech", "slovakia", "romania", "bulgaria",
    "greece", "cyprus", "malta", "estonia", "latvia", "lithuania", "hungary",
    "slovenia", "croatia", "serbia", "bosnia", "montenegro", "albania",
    "macedonia", "moldova", "ukraine", "georgia", "armenia"
]

def _text(s): return (s or "").strip()

def _has_any(text: str, needles: list[str]) -> bool:
    if not text or not needles: return False
    t = text.lower()
    return any((n or "").lower() in t for n in needles)

def _location_ok(job: dict, kw: dict) -> bool:
    allow_unlocated = kw.get("allow_unlocated", True)
    loc_cfg = kw.get("location") or {}
    allow = loc_cfg.get("include") or kw.get("location_allowlist") or []
    deny  = loc_cfg.get("exclude") or []
    loc = _text(job.get("location"))
    if not loc:
        return bool(allow_unlocated)
    if allow and not _has_any(loc, allow):
        return False
    if deny and _has_any(loc, deny):
        return False
    return True

def _score(job: dict, kw: dict) -> float:
    title, company, location = _text(job.get("title")), _text(job.get("company")), _text(job.get("location"))
    hay = f"{title} | {company} | {location}"
    inc, exc = kw.get("include") or [], kw.get("exclude") or []
    remote_terms, emea_terms = kw.get("remote_terms") or DEFAULT_REMOTE_TERMS, kw.get("emea_terms") or DEFAULT_EMEA_TERMS

    score = 0.0
    for k in inc:
        if _has_any(hay, [k]): score += 1
    for k in exc:
        if _has_any(hay, [k]): score -= 0.5
    if _has_any(location, remote_terms): score += 2
    if _has_any(location, emea_terms): score += 1.5
    if _has_any(title, ["music", "audio", "catalog", "metadata", "royalties", "licensing"]): score += 0.5
    if _text(job.get("posted_at")): score += 0.2
    return score

def filter_jobs(jobs: list[dict], kw: dict) -> list[dict]:
    if not isinstance(jobs, list): return []
    inc, exc = kw.get("include") or [], kw.get("exclude") or []
    out = []
    for j in jobs:
        title, company, location = _text(j.get("title")), _text(j.get("company")), _text(j.get("location"))
        hay = f"{title} | {company} | {location}"
        if inc and not _has_any(hay, inc): continue
        if exc and _has_any(hay, exc): continue
        if not _location_ok(j, kw): continue
        jj = dict(j)
        jj["score"] = _score(j, kw)
        out.append(jj)
    out.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return out
