import os, yaml, datetime as dt
from jinja2 import Environment, FileSystemLoader
from email.utils import formatdate
from email.mime.text import MIMEText
import smtplib

from vendors.normalize import normalize
from vendors.dedupe import dedupe
from vendors.filters import filter_jobs

from adapters import lever, greenhouse, rss, workable, ashby

ADAPTERS = {
    "lever": lever,
    "greenhouse": greenhouse,
    "workable": workable,
    "ashby": ashby,      # now real adapter
    "workday": rss,      # temporary until next step
    "rss": rss,
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

    all_jobs=[]
    for c in companies["targets"]:
        ats = c.get("ats")
        adapter = ADAPTERS.get(ats)
        if not adapter:
            print("SKIP", c.get("name"), "unsupported ats:", ats); continue
        try:
            jobs = adapter.fetch(c)
            all_jobs.extend(jobs)
            print(f"{c['name']}: {len(jobs)}")
        except Exception as e:
            print("ERROR", c.get("name"), e)

    all_jobs = normalize(all_jobs)
    all_jobs = filter_jobs(all_jobs, kw)
    all_jobs = dedupe(all_jobs)

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
