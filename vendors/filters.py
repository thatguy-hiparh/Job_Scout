# vendors/filters.py — full replacement
from __future__ import annotations
import datetime as dt
from typing import List, Dict, Any
from dateutil import parser

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

def _company_kw(job: dict, kw: dict) -> dict:
    companies = kw.get("companies") or {}
    company = _text(job.get("company"))
    if not company or not companies:
        return {}
    for name, cfg in companies.items():
        if _has_any(company, [name]):
            return cfg or {}
    return {}

def _merge_kw(base: dict, override: dict) -> dict:
    merged = dict(base)
    merged["include"] = (base.get("include") or []) + (override.get("include") or [])
    merged["exclude"] = (base.get("exclude") or []) + (override.get("exclude") or [])
    return merged

def _is_too_old(job: dict, kw: dict) -> bool:
    max_age_days = kw.get("max_age_days", 30)
    if max_age_days is None:
        return False
    posted_at = job.get("posted_at")
    if not posted_at:
        return False
    try:
        posted_dt = parser.parse(str(posted_at))
    except Exception:
        return False
    if not posted_dt.tzinfo:
        posted_dt = posted_dt.replace(tzinfo=dt.timezone.utc)
    now = dt.datetime.now(dt.timezone.utc)
    delta = now - posted_dt
    if delta.total_seconds() < 0:
        return False
    return delta.days > max_age_days

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
    out = []
    for j in jobs:
        company_kw = _company_kw(j, kw)
        kw_local = _merge_kw(kw, company_kw)
        inc, exc = kw_local.get("include") or [], kw_local.get("exclude") or []
        title, company, location = _text(j.get("title")), _text(j.get("company")), _text(j.get("location"))
        hay = f"{title} | {company} | {location}"
        if inc and not _has_any(hay, inc): continue
        if exc and _has_any(hay, exc): continue
        if _is_too_old(j, kw_local): continue
        if not _location_ok(j, kw_local): continue
        jj = dict(j)
        jj["score"] = _score(j, kw_local)
        out.append(jj)
    out.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return out
