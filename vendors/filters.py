def filter_jobs(jobs, kw):
    pos = [w.lower() for w in kw.get("include_keywords",[])]
    neg = [w.lower() for w in kw.get("exclude_keywords",[])]
    geo = kw.get("geo",{})

    def hit(text, bag):
        t = (text or "").lower()
        return any(w in t for w in bag)

    out=[]
    for j in jobs:
        text = " ".join(filter(None,[
            j.get("title"), j.get("location"),
            j.get("department"), j.get("team"),
            j.get("description_snippet")
        ]))
        if pos and not hit(text, pos):
            continue
        if neg and hit(text, neg):
            continue

        loc = (j.get("location") or "").lower()
        if geo:
            allow=False
            if geo.get("allow_remote") and "remote" in loc:
                allow=True
            if any(x.lower() in loc for x in geo.get("countries",[])):
                allow=True
            if any(x.lower() in loc for x in geo.get("cities",[])):
                allow=True
            if not allow:
                continue
        out.append(j)
    return out
