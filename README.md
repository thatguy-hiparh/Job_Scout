# Job Scout (Music/Audio Sector)

Daily (09:00 Italy) aggregator of jobs from ATS backends (Lever, Greenhouse, etc.), filtered for music/audio/rights/QA/ops, delivered as:
- HTML report on GitHub Pages (`docs/daily_report.html`)
- Email (SMTP)

## Quick start
1. Create a repo; push this tree to `main`.
2. Add Secrets (Settings → Secrets → Actions):
   - `SMTP_HOST` (e.g., smtp.gmail.com)
   - `SMTP_PORT` (587)
   - `SMTP_USER` (your Gmail / SMTP user)
   - `SMTP_PASS` (app password)
   - `EMAIL_TO` (your email)
3. Enable Pages: Settings → Pages → "Build and deployment" → "GitHub Actions".
4. Manually run the workflow (Actions → job_scout_daily → Run).
5. Extend `config/companies.yml` with more targets; add adapters as needed.

## Local run
```
pip install -r requirements.txt
python main.py --out docs/daily_report.html
```

## Adapters
- Implement new ATS modules in `adapters/`; add to `ADAPTERS` map in `main.py`.

## Notes
- Respect robots/TOS. Prefer official JSON endpoints.
- Expand `keywords.yml` to tune signal.
