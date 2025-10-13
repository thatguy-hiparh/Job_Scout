import os
import yaml
import datetime as dt
import traceback
from jinja2 import Environment, FileSystemLoader
from email.utils import formatdate
from email.mime.text import MIMEText
import smtplib

from vendors.normalize import normalize
from vendors.dedupe import dedupe
from vendors.filters import filter_jobs

from adapters import (
    lever,
    greenhouse,
    rss,
    workable,
    ashby,
    workday,
    workday_gql,
    smartrecruiters,
    randstad_it,
    adecco_it,
    workday_pw,
)

# Map ATS string -> adapter module (module must expose .fetch(company) -> list[dict])
ADAPTERS = {
    "lever":           lever,
    "greenhouse":      greenhouse,
    "workable":        workable,
    "ashby":           ashby,
    "workday":         workday,
    "workday_gql":     workday_gql,
    "workday_pw":      workday_pw,
    "workday-pw":      workday_pw,   # alias
    "smartrecruiters": smartrecruiters,
    "rss":             rss,
    "randstad_it":     randstad_it,
    "adecco_it":       adecco_it,
}

def send_email(html: str) -> None:
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    pwd  = os.getenv("SMTP_PASS")
    to   = os.getenv("EMAIL_TO")

    if not (host and user and pwd and to):
        print("EMAIL: missing SMTP_* or EMAIL_TO; skipping email")
        return

    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = "Job Scout — Daily Report"
    msg["From"] = user
    msg["To"] = to
    msg["Date"] = formatdate(localtime=True)

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.sendmail(user, [to], msg.as_string())
    print("EMAIL: sent")

def render_html(jobs, outpath: str) -> str:
    env = Environment(loader=FileSystemLoader("outputs/templates"))
    tpl = env.get_template("daily_report.html.j2")
    ts = dt.datetime.now().isoformat(timespec="seconds")
    html = tpl.render(generated_at=ts, total=len(jobs), jobs=jobs)
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"WROTE: {outpath}")
    return html

def _split_env_list(name: str):
    raw = os.getenv(name, "")
    return [t.strip() for t in raw.split(",") if t.strip()]

def _text_from_job(job: dict) -> str:
    parts = [
        job.get("location", ""),
        job.get("country", ""),
        job.get("title", ""),
        job.get("department", ""),
        job.get("company", ""),
    ]
    return " | ".join([p for p in parts if p]).lower()

def _passes_location(job: dict, allow_terms, deny_terms) -> bool:
    if not allow_terms and not deny_terms:
        return True  # nothing to enforce

    blob = _text_from_job(job)

    # deny has priority – if a deny term is present, reject
    for d in deny_terms:
        if d.lower() in blob:
            return False

    # if allow list is provided, require at least one match
    if allow_terms:
        for a in allow_terms:
            if a.lower() in blob:
                return True
        return False

    return True

def run(companies_file: str, keywords_file: str, outpath: str) -> None:
    companies = yaml.safe_load(open(companies_file, encoding="utf-8"))
    kw = yaml.safe_load(open(keywords_file, encoding="utf-8"))

    # Toggle: bypass keyword filter to see *all* scraped jobs
    skip_filter = os.getenv("SKIP_FILTER") == "1"
    if skip_filter:
        print("DEBUG: SKIP_FILTER=1 — report will include ALL scraped jobs (no keyword filtering)")

    # NEW: simple location allow/deny via env
    allow_locations = _split_env_list("ALLOW_LOCATIONS")
    deny_locations  = _split_env_list("DENY_LOCATIONS")
    if allow_locations or deny_locations:
        print(f"INFO: Location filter active. ALLOW={allow_locations}  DENY={deny_locations}")

    all_jobs = []
    per_company_counts = []

    for c in companies["targets"]:
        name = c.get("name", "UNKNOWN")
        ats = c.get("ats")
        adapter = ADAPTERS.get(ats)

        if not adapter:
            print("SKIP", name, "unsupported ats:", ats)
            per_company_counts.append(f"{name}=SKIP")
            continue

        try:
            jobs = adapter.fetch(c) or []
            # apply location filter immediately to reduce downstream load
            if allow_locations or deny_locations:
                before = len(jobs)
                jobs = [j for j in jobs if _passes_location(j, allow_locations, deny_locations)]
                after = len(jobs)
                if before != after:
                    print(f"INFO: {name}: location filter kept {after}/{before}")

            all_jobs.extend(jobs)
            print(f"{name}: {len(jobs)}")
            per_company_counts.append(f"{name}={len(jobs)}")
        except Exception as e:
            print("ERROR", name, e)
            traceback.print_exc(limit=1)
            per_company_counts.append(f"{name}=ERR")

    # Normalize and (optionally) keyword-filter/dedupe
    all_jobs = normalize(all_jobs)

    if not skip_filter:
        all_jobs = filter_jobs(all_jobs, kw)
    else:
        print(f"DEBUG: keyword filter skipped — {len(all_jobs)} jobs before dedupe")

    before_dedupe = len(all_jobs)
    all_jobs = dedupe(all_jobs)
    after_dedupe = len(all_jobs)
    if after_dedupe != before_dedupe:
        print(f"INFO: Dedupe removed {before_dedupe - after_dedupe} duplicates")

    html = render_html(all_jobs, outpath)
    send_email(html)

    print("SUMMARY:", " | ".join(per_company_counts), "| Total=", after_dedupe)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/companies.yml")
    ap.add_argument("--keywords", default="config/keywords.yml")
    ap.add_argument("--out", default="docs/daily_report.html")
    args = ap.parse_args()
    run(args.config, args.keywords, args.out)
