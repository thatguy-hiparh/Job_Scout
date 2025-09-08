import feedparser
from urllib.parse import urljoin

def fetch(company):
    base = company.get("homepage") or ""
    candidates = [company.get("rss",""), "/feed", "/rss", "/atom.xml", "/blog/rss"]
    jobs=[]
    for path in candidates:
        if not path: continue
        url = path if path.startswith("http") else urljoin(base, path)
        feed = feedparser.parse(url)
        if not feed or not feed.entries: 
            continue
        for e in feed.entries:
            jobs.append({
                "source": "rss",
                "company": company["name"],
                "id": getattr(e, "id", getattr(e, "link", None)),
                "title": getattr(e, "title", None),
                "location": None,
                "remote": False,
                "department": None,
                "team": None,
                "url": getattr(e, "link", None),
                "posted_at": getattr(e, "published", None),
                "description_snippet": getattr(e, "summary","")[:240]
            })
        if jobs: break
    return jobs
