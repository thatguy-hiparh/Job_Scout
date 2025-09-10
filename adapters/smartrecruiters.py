import httpx, os, json
from tenacity import retry, wait_exponential, stop_after_attempt

BASE = "https://api.smartrecruiters.com/v1/companies/{slug}/postings"

HEADERS = {
    "User-Agent": "job-scout/1.0 (+https://github.com/thatguy-hiparh/Job_Scout)",
    "Accept": "application/json, text/plain, */*",
}

# Minimal name→ISO2 map for common EU/US regions we care about
NAME_TO_ISO2 = {
    "italy": "it",
    "ireland": "ie",
    "united kingdom": "gb",
    "uk": "gb",
    "great britain": "gb",
    "england": "gb",
    "scotland": "gb",
    "wales": "gb",
    "germany": "de",
    "spain": "es",
    "france": "fr",
    "netherlands": "nl",
    "portugal": "pt",
    "switzerland": "ch",
    "austria": "at",
    "belgium": "be",
    "luxembourg": "lu",
    "poland": "pl",
    "czechia": "cz",
    "czech republic": "cz",
    "romania": "ro",
    "united states": "us",
    "usa": "us",
    "u.s.": "us",
    "u.s.a.": "us",
}

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def _get(url, params=None, timeout=30):
    with httpx.Client(headers=HEADERS, timeout=timeout, follow_redirects=True) as c:
        return c.get(url, params=params or {})

def _to_loc(locobj):
    if not isinstance(locobj, dict):
        return ""
    parts = [locobj.get("city"), locobj.get("region"), locobj.get("country")]
    return ", ".join([p for p in parts if p])

def _country(locobj):
    if isinstance(locobj, dict):
        return (locobj.get("country") or "").strip()
    return ""

def _city(locobj):
    if isinstance(locobj, dict):
        return (locobj.get("city") or "").strip()
    return ""

def _iter_company_slugs(company):
    slugs = []
    multi = company.get("smartrecruiters_slugs")
    if isinstance(multi, list) and multi:
        slugs.extend([s for s in multi if isinstance(s, str) and s.strip()])
    s = company.get("slug")
    if isinstance(s, str) and s.strip():
        if s not in slugs:
            slugs.append(s)
    if not slugs and company.get("name"):
        slugs.append(company["name"])
    return slugs

def _normalize_country_tokens(c):
    """
    Return a set of tokens that represent this country in common forms:
      e.g., "Italy" -> {"italy","it"}, "IT" -> {"it","italy"}
    """
    c = (c or "").strip().lower()
    if not c:
        return set()
    toks = {c}
    # If name → ISO
    if len(c) > 2:
        iso = NAME_TO_ISO2.get(c)
        if iso:
            toks.add(iso)
    # If ISO → try name (reverse lookup)
    if len(c) == 2:
        # add any names that map to this ISO
        for name, code in NAME_TO_ISO2.items():
            if code == c:
                toks.add(name)
    return toks

def fetch(company):
    debug = os.getenv("DEBUG_SMART","").strip().lower() in ("1","true","yes","on")
    attempts = []

    slugs = _iter_company_slugs(company)
    if not slugs:
        return []

    limit = 100
    results = []

    # Build allowed country token set that includes BOTH names and ISO2 codes
    raw_countries = [c for c in company.get("smartrecruiters_countries", []) if isinstance(c, str)]
    allowed_country_tokens = set()
    for rc in raw_countries:
        allowed_country_tokens |= _normalize_country_tokens(rc)

    # Cities (we'll do substring match, case-insensitive)
    allowed_cities = set([c.lower() for c in company.get("smartrecruiters_cities", []) if isinstance(c, str)])

    def allow_by_geo(locobj):
        # If no geo constraints, allow everything
        if not allowed_country_tokens and not allowed_cities:
            return True
        ctry = _country(locobj)
        city = _city(locobj).lower()
        loc_str = _to_loc(locobj).lower()
        if "remote" in loc_str:
            return True

        # country OK if *any* token of the country matches allowed tokens
        ctry_tokens = _normalize_country_tokens(ctry)
        country_ok = bool(allowed_country_tokens & ctry_tokens) if allowed_country_tokens else False

        # city OK if any allowed city is a substring of the city string
        city_ok = any(x in city for x in allowed_cities) if allowed_cities else False

        # OR logic: country OR city
        return country_ok or city_ok

    for slug in slugs:
        url = BASE.format(slug=slug)
        offset = 0
        seen_ids = set()
        got_for_slug = 0

        while True:
            try:
                r = _get(url, params={"limit": limit, "offset": offset})
                ct = r.headers.get("Content-Type","").lower()
                if r.status_code != 200 or "json" not in ct:
                    attempts.append({"slug": slug, "status": r.status_code, "json": ("json" in ct), "items": 0})
                    break
                data = r.json() or {}
                items = data.get("content") or data.get("postings") or []
                if not isinstance(items, list) or not items:
                    attempts.append({"slug": slug, "status": r.status_code, "json": True, "items": 0})
                    break

                count_this_page = 0
                for p in items:
                    if not isinstance(p, dict):
                        continue
                    if not allow_by_geo(p.get("location") or {}):
                        continue

                    pid   = p.get("id") or p.get("refNumber")
                    if pid in seen_ids:
                        continue
                    seen_ids.add(pid)

                    title = p.get("name")
                    loc   = _to_loc(p.get("location") or {})
                    posted= p.get("releasedDate") or p.get("createdOn")
                    url2  = p.get("applyUrl") or p.get("ref") or p.get("jobAdUrl") or p.get("jobUrl")

                    snippet = ""
                    ad = p.get("jobAd") or {}
                    if isinstance(ad, dict):
                        sections = ad.get("sections") or []
                        if isinstance(sections, list) and sections:
                            first = sections[0] or {}
                            snippet = (first.get("text") or "")[:240]

                    results.append({
                        "source": "smartrecruiters",
                        "company": company["name"],
                        "id": str(pid) if pid is not None else None,
                        "title": title,
                        "location": loc,
                        "remote": isinstance(loc, str) and ("remote" in loc.lower()),
                        "department": p.get("department") or None,
                        "team": None,
                        "url": url2,
                        "posted_at": posted,
                        "description_snippet": snippet,
                    })
                    count_this_page += 1

                got_for_slug += count_this_page
                attempts.append({"slug": slug, "status": r.status_code, "json": True, "items": count_this_page})

                if len(items) < limit:
                    break
                offset += limit
            except Exception:
                attempts.append({"slug": slug, "status": "error", "json": False, "items": 0})
                break

        # if we got something for this slug, no need to try alternates
        if got_for_slug > 0:
            continue

    if debug:
        print(f"SMART_DEBUG {company['name']}: {json.dumps(attempts)[:1800]} got={len(results)}")

    return results
