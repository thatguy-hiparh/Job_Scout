import os, json, httpx, re
from tenacity import retry, wait_exponential, stop_after_attempt

BASE = "https://api.smartrecruiters.com/v1/companies/{slug}/postings"

HEADERS = {
    "User-Agent": "job-scout/1.0 (+https://github.com/thatguy-hiparh/Job_Scout)",
    "Accept": "application/json, text/plain, */*",
}

# Name → ISO2 (extend if needed)
NAME_TO_ISO2 = {
    "italy": "it", "italia": "it",
    "ireland": "ie",
    "united kingdom": "gb", "uk": "gb", "great britain": "gb", "england": "gb", "scotland": "gb", "wales": "gb",
    "germany": "de", "spain": "es", "france": "fr", "netherlands": "nl", "portugal": "pt", "switzerland": "ch",
    "austria": "at", "belgium": "be", "luxembourg": "lu", "poland": "pl", "czechia": "cz", "czech republic": "cz",
    "romania": "ro",
    "united states": "us", "usa": "us", "u.s.": "us", "u.s.a.": "us",
}

ITALY_CITY_TOKENS = [
    "milan", "milano", "rome", "roma", "turin", "torino", "bologna", "florence", "firenze",
    "naples", "napoli", "genoa", "genova", "venice", "venezia", "palermo", "bari", "verona",
    "parma", "modena", "padova", "pisa", "trieste", "trento", "bolzano", "bergamo", "brescia",
    "cagliari", "catania", "messina", "lecce", "rimini", "perugia", "reggio emilia", "udine",
]

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, params=None, timeout=30):
    with httpx.Client(headers=HEADERS, timeout=timeout, follow_redirects=True) as c:
        return c.get(url, params=params or {})

def _val(v):
    if v is None: return ""
    if isinstance(v, (int, float)): return str(v)
    return str(v).strip()

def _pick_dict(d, keys):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return None

def _country_value(locobj):
    if not isinstance(locobj, dict): return ""
    for k in ("country", "countryCode", "country_code"):
        v = locobj.get(k)
        if isinstance(v, str): return _val(v)
        if isinstance(v, dict):
            vv = _pick_dict(v, ("code", "id", "name", "label", "value"))
            if vv: return _val(vv)
    v = locobj.get("country")
    if isinstance(v, dict):
        vv = _pick_dict(v, ("code", "id", "name", "label", "value"))
        if vv: return _val(vv)
    return ""

def _city_value(locobj):
    if not isinstance(locobj, dict): return ""
    for k in ("city", "name", "label"):
        v = locobj.get(k)
        if isinstance(v, str): return _val(v)
        if isinstance(v, dict):
            vv = _pick_dict(v, ("name", "label", "value", "city"))
            if vv: return _val(vv)
    return ""

def _region_value(locobj):
    if not isinstance(locobj, dict): return ""
    for k in ("region", "state", "province"):
        v = locobj.get(k)
        if isinstance(v, str): return _val(v)
        if isinstance(v, dict):
            vv = _pick_dict(v, ("name", "label", "value", "code", "id"))
            if vv: return _val(vv)
    return ""

def _location_str(locobj):
    if not isinstance(locobj, dict): return ""
    parts = [_city_value(locobj), _region_value(locobj), _country_value(locobj)]
    return ", ".join([p for p in parts if p]).strip()

def _normalize_country_tokens(c):
    c = _val(c).lower()
    if not c: return set()
    toks = {c}
    if len(c) > 2:
        iso = NAME_TO_ISO2.get(c)
        if iso: toks.add(iso)
    if len(c) == 2:
        for name, code in NAME_TO_ISO2.items():
            if code == c: toks.add(name)
    return toks

def _iter_company_slugs(company):
    slugs = []
    multi = company.get("smartrecruiters_slugs")
    if isinstance(multi, list) and multi:
        slugs.extend([s for s in multi if isinstance(s, str) and s.strip()])
    s = company.get("slug")
    if isinstance(s, str) and s.strip() and s not in slugs:
        slugs.append(s)
    if not slugs and company.get("name"):
        slugs.append(company["name"])
    return slugs

def fetch(company):
    debug = os.getenv("DEBUG_SMART","").strip().lower() in ("1","true","yes","on")
    attempts = []

    slugs = _iter_company_slugs(company)
    if not slugs:
        return []

    # Allowed countries & cities
    raw_countries = [c for c in company.get("smartrecruiters_countries", []) if isinstance(c, str)]
    allowed_country_tokens = set()
    allowed_country_names_lower = set()
    for rc in raw_countries:
        allowed_country_tokens |= _normalize_country_tokens(rc)
        allowed_country_names_lower.add(_val(rc).lower())
    allowed_cities = set([c.lower() for c in company.get("smartrecruiters_cities", []) if isinstance(c, str)])

    limit = 100

    def allow_by_geo(locobj):
        # No constraints → allow all
        if not allowed_country_tokens and not allowed_cities:
            return True

        loc_str = _location_str(locobj).lower()
        if "remote" in loc_str:
            return True

        ctry = _country_value(locobj)
        ctry_tokens = _normalize_country_tokens(ctry)
        country_ok = bool(allowed_country_tokens & ctry_tokens) if allowed_country_tokens else False

        city = _city_value(locobj).lower()
        city_ok = any(x in city for x in allowed_cities) if allowed_cities else False

        # Fallback: country name present in the free-form location string
        name_in_loc = any(name in loc_str for name in allowed_country_names_lower) if allowed_country_names_lower else False

        return country_ok or city_ok or name_in_loc

    def fetch_one(slug, geo_filter=True):
        url = BASE.format(slug=slug)
        offset = 0
        seen = set()
        out = []
        got = 0
        while True:
            r = _get(url, params={"limit": limit, "offset": offset})
            ct = (r.headers.get("Content-Type") or "").lower()
            if r.status_code != 200 or "json" not in ct:
                attempts.append({"slug": slug, "status": r.status_code, "json": ("json" in ct), "items": 0})
                break
            data = r.json() or {}
            items = data.get("content") or data.get("postings") or []
            if not isinstance(items, list) or not items:
                attempts.append({"slug": slug, "status": r.status_code, "json": True, "items": 0})
                break

            count_page = 0
            for p in items:
                if not isinstance(p, dict):
                    continue
                if geo_filter and not allow_by_geo(p.get("location") or {}):
                    continue

                pid = p.get("id") or p.get("refNumber")
                if pid in seen:
                    continue
                seen.add(pid)

                title = _val(p.get("name"))
                loc   = _location_str(p.get("location") or {})
                posted= p.get("releasedDate") or p.get("createdOn")
                url2  = p.get("applyUrl") or p.get("ref") or p.get("jobAdUrl") or p.get("jobUrl")

                snippet = ""
                ad = p.get("jobAd") or {}
                if isinstance(ad, dict):
                    sections = ad.get("sections") or []
                    if isinstance(sections, list) and sections:
                        first = sections[0] or {}
                        snippet = (_val(first.get("text")))[:240]

                out.append({
                    "source": "smartrecruiters",
                    "company": company["name"],
                    "id": str(pid) if pid is not None else None,
                    "title": title,
                    "location": loc,
                    "remote": "remote" in loc.lower(),
                    "department": p.get("department") or None,
                    "team": None,
                    "url": url2,
                    "posted_at": posted,
                    "description_snippet": snippet,
                })
                count_page += 1

            got += count_page
            attempts.append({"slug": slug, "status": r.status_code, "json": True, "items": count_page})
            if len(items) < limit:
                break
            offset += limit
        return out, got

    # 1) First pass: normal geo-filtered
    results = []
    for slug in slugs:
        out, _ = fetch_one(slug, geo_filter=True)
        results.extend(out)

    # 2) If nothing found, raw probe and post-filter by text (Italy/Italia + cities)
    if not results:
        raw_summary = []
        total = 0
        raw_items = []
        for slug in slugs:
            out, got = fetch_one(slug, geo_filter=False)
            total += got
            raw_summary.append({"slug": slug, "raw_items": got})
            raw_items.extend(out)

        if debug:
            print(f"SMART_RAW {company['name']}: {json.dumps(raw_summary)} total_raw={total}")

        if raw_items and (allowed_country_tokens or allowed_cities):
            # Build text tokens from country names + Italy cities
            tokens = set(allowed_country_names_lower) | set(ITALY_CITY_TOKENS)
            # also include ISO2 forms like 'it'
            tokens |= set([t for t in allowed_country_tokens if len(t) <= 3])
            def _hits(job):
                hay = " ".join([
                    (job.get("location") or ""),
                    (job.get("title") or ""),
                    (job.get("description_snippet") or ""),
                    (job.get("url") or ""),
                ]).lower()
                return any(tok in hay for tok in tokens)
            results = [j for j in raw_items if _hits(j)]

    if debug:
        print(f"SMART_DEBUG {company['name']}: {json.dumps(attempts)[:1800]} got={len(results)}")

    return results
