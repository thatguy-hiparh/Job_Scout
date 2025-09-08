from dateutil import parser

def _norm(s):
    return s.strip() if isinstance(s,str) else s

def normalize(jobs):
    out=[]
    for j in jobs:
        jj = dict(j)
        jj["title"] = _norm(j.get("title"))
        jj["company"] = _norm(j.get("company"))
        jj["location"] = _norm(j.get("location"))
        jj["url"] = _norm(j.get("url"))
        pa = j.get("posted_at")
        if isinstance(pa,str):
            try:
                jj["posted_at"] = parser.parse(pa).isoformat()
            except Exception:
                pass
        out.append(jj)
    return out
