from rapidfuzz import fuzz

def dedupe(jobs):
    seen = set()
    unique=[]
    for j in jobs:
        key = (j.get("source"), str(j.get("id")))
        if key in seen or j.get("id") is None:
            continue
        seen.add(key)
        unique.append(j)

    final=[]
    for j in unique:
        dup=False
        for k in final:
            if j["company"]==k["company"]:
                if fuzz.WRatio((j.get("title") or ""), (k.get("title") or "")) >= 92:
                    if (j.get("location") or "") == (k.get("location") or ""):
                        dup=True; break
        if not dup: final.append(j)
    return final
