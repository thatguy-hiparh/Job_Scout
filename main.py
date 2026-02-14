import os, yaml, datetime as dt
from jinja2 import Environment, FileSystemLoader
from email.utils import formatdate
from email.mime.text import MIMEText
import smtplib

from vendors.normalize import normalize
from vendors.dedupe import dedupe
from vendors.filters import filter_jobs_with_debug

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

# Map ATS string -> adapter module
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

def send_email(html):
    host=os.getenv("SMTP_HOST"); port=int(os.getenv("SMTP_PORT","587"))
    user=os.getenv("SMTP_USER"); pwd=os.getenv("SMTP_PASS"); to=os.getenv("EMAIL_TO")
    if not (host and user and pwd and to):
        print("EMAIL: missing SMTP_* or EMAIL_TO; skipping email")
        return
    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = "Job Scout — Daily Report"
    msg["From"] = user; msg["To"] = to; msg["Date"]=formatdate(localtime=True)
    with smtplib.SMTP(host, port) as s:
        s.starttls(); s.login(user, pwd); s.sendmail(user, [to], msg.as_string())
    print("EMAIL: sent")

def render_html(jobs, outpath):
    env = Environment(loader=FileSystemLoader("outputs/templates"))
    tpl = env.get_template("daily_report.html.j2")
    ts = dt.datetime.now().isoformat(timespec="seconds")
    html = tpl.render(generated_at=ts, total=len(jobs), jobs=jobs)
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w", encoding="utf-8") as f: f.write(html)
    print(f"WROTE: {outpath}")
    return html

def run(companies_file, keywords_file, outpath):
    companies = yaml.safe_load(open(companies_file, encoding="utf-8"))
    kw = yaml.safe_load(open(keywords_file, encoding="utf-8"))
    targets = companies.get("targets", [])

    skip_filters = (os.getenv("SKIP_FILTERS", "").strip().lower() in {"1", "true", "yes", "on"})

    print(f"FUNNEL: companies_loaded={len(targets)}")

    all_jobs=[]
    for c in targets:
        ats = c.get("ats")
        adapter = ADAPTERS.get(ats)
        if not adapter:
            print("SKIP", c.get("name"), "unsupported ats:", ats); continue
        try:
            jobs = adapter.fetch(c)
            all_jobs.extend(jobs)
            print(f"FETCH: company={c['name']} adapter={ats} jobs={len(jobs)}")
        except Exception as e:
            print("ERROR", c.get("name"), e)

    print(f"FUNNEL: total_fetched_before_normalize={len(all_jobs)}")
    all_jobs = normalize(all_jobs)
    print(f"FUNNEL: total_before_filtering={len(all_jobs)}")

    # Allow bypassing filters to debug: set SKIP_FILTERS=1
    if skip_filters:
        print("FILTERS: skipped (SKIP_FILTERS=1)")
    else:
        before = len(all_jobs)
        all_jobs, debug = filter_jobs_with_debug(all_jobs, kw)
        print(f"FUNNEL: total_after_filtering={len(all_jobs)} (removed {before - len(all_jobs)})")

        if len(all_jobs) == 0:
            reasons = debug.get("reasons") or {}
            if reasons:
                print("FILTER_DEBUG: top_rejection_reasons")
                for i, (reason, count) in enumerate(list(reasons.items())[:10], start=1):
                    print(f"FILTER_DEBUG:   {i}. {reason}: {count}")
            else:
                print("FILTER_DEBUG: no rejection reasons captured")

            examples = debug.get("examples") or []
            if examples:
                print("FILTER_DEBUG: rejected_examples")
                for i, ex in enumerate(examples[:3], start=1):
                    print(
                        "FILTER_DEBUG:   "
                        f"{i}. reason={ex.get('reason')} | title={ex.get('title')} | "
                        f"company={ex.get('company')} | location={ex.get('location')} | "
                        f"posted_at={ex.get('posted_at')} | source={ex.get('source')} | id={ex.get('id')}"
                    )

    all_jobs = dedupe(all_jobs)
    print(f"FUNNEL: total_after_dedupe={len(all_jobs)}")

    html = render_html(all_jobs, outpath)
    send_email(html)

if __name__ == "__main__":
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--config", default="config/companies.yml")
    ap.add_argument("--keywords", default="config/keywords.yml")
    ap.add_argument("--out", default="docs/daily_report.html")
    args=ap.parse_args()
    run(args.config, args.keywords, args.out)
