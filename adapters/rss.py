# adapters/rss.py
import re
from urllib.parse import urlparse
import feedparser
from datetime import datetime

# Heuristics to keep only job-ish posts from RSS
_ALLOW_PATH_PATTERNS = [
    r"/careers?(/|$)", r"/jobs?(/|$)", r"/openings?(/|$)",
    r"/join[-_]us(/|$)", r"/work[-_]with[-_]us(/|$)", r"/positions?(/|$)",
    r"/vacancies?(/|$)", r"/opportunit(y|ies)(/|$)",
]
_ALLOW_TITLE_PATTERNS = [
    r"\b(hiring|we're hiring|we are hiring)\b",
    r"\b(role|position|opening|vacancy|career)\b",
    r"\b(engineer|developer|data|ml|ai|product|designer|marketer|analyst|manager|producer|a&r|audio)\b",
    r"\bintern(ship)?\b",
]

# Hard denylist for common non-jobs RSS sections
_DENY_DOMAINS_OR_PATHS = [
    "apple.com/newsroom",
    "theorchard.com/press",
    "epidemicsound.com/blog",
    "newsroom",
    "press",
    "blog",
    "stories",
    "insights",
    "podcast",
    "updates",
]

_allow_path_re = re.compile("|".join(_ALLOW_PATH_PATTERNS), re.I)
_allow_title_re = re.compile("|".join(_ALLOW_TITLE_PATTERNS), re.I)

def _looks_like_job(link: str, title: str) -> bool:
    if not link:
        return False
    p = urlparse(link)
    host_path = f"{p.netloc}{p.path}".lower()

    # deny if clearly a newsroom/press/blog/etc
    for frag in _DENY_DOMAINS_OR_PATHS:
        if frag in host_path:
            return False

    # allow by path or by title cues
    path_ok = bool(_allow_path_re.search(p.path or ""))
    title_ok = bool(_allow_title_re.search(title or ""))
    return path_ok or title_ok

def _normalize_entry(e, company_name: str):
    link = (e.get("link") or "").strip()
    title = (e.get("title") or "").strip()
    summary = (e.get("summary") or "").strip()
    published = None
    if "published_parsed" in e and e.published_parsed:
        published = datetime(*e.published_parsed[:6]).isoformat()

    return {
        "company": company_name,
        "title": title or "(untitled)",
        "location": None,              # RSS rarely has structured location
        "url": link,
        "source": "rss",
        "posted_at": published,
        "description": summary,
    }

def fetch(company: dict) -> list:
    """
    Company config:
      - name: Apple
        ats: rss
        rss_feeds:
          - https://…/rss
          - https://…/another.xml
    """
    feeds = company.get("rss_feeds") or company.get("rss_urls") or []
    results = []
    for feed_url in feeds:
        try:
            d = feedparser.parse(feed_url)
            for e in d.entries:
                link = (e.get("link") or "").strip()
                title = (e.get("title") or "").strip()
                if _looks_like_job(link, title):
                    results.append(_normalize_entry(e, company.get("name")))
        except Exception:
            # fail quiet; other adapters continue
            continue
    return results
