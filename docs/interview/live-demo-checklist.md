# Live Demo Checklist

Use this before an interview or reviewer walkthrough. The goal is to make the
demo reliable, with a graceful fallback if Snowflake, the API, or the browser is
slow.

## Five Minutes Before

From the repo root:

```bash
git status --short --branch
```

Expected: clean branch, pushed to GitHub.

Check the API:

```bash
curl -sS http://127.0.0.1:8000/health
```

Expected: `status` is `ok`.

Check the dashboard:

```bash
curl -I http://localhost:8501/
```

Expected: HTTP 200.

Check the README visuals:

```bash
ls -lh docs/assets/*.png
```

Expected:

- `atlas-architecture.png`
- `dashboard-executive-overview.png`
- `residual-review-report.png`

## Demo Tabs To Open

- GitHub README
- `docs/interview/one-page-brief.md`
- `docs/interview/edward-demo-talk-track.md`
- `docs/interview/productionization-and-metric-catalog.md`
- Streamlit dashboard at `http://localhost:8501`
- FastAPI docs at `http://127.0.0.1:8000/docs`
- Residual review report at `docs/walkthroughs/residual-review-report.md`

## Live Demo Path

1. Start from the README `Start Here For Reviewers` section.
2. Show the architecture image.
3. Open the dashboard executive overview.
4. Point to k-anonymity suppression and the data-through date.
5. Open the API docs and show the privacy-safe metric endpoints.
6. Open the residual review report and explain that it is review-only.
7. Close on the productionization and 30/60/90 docs.

## Fallback Path

If the live app is slow:

1. Use `docs/assets/dashboard-executive-overview.png`.
2. Use `docs/assets/atlas-architecture.png`.
3. Use `docs/walkthroughs/residual-review-report.md`.
4. Narrate from `docs/interview/two-minute-demo-script.md`.

Strong fallback phrase:

> The live surface is just one consumer. The core project is the governed data
> foundation: identity, history, privacy, tests, and documented tradeoffs.

## Do Not Show

- `.env`
- Snowflake credentials
- local virtual environment internals
- generated CSVs under `seeds/output/`
- any terminal output containing secrets

## Final Check

The safest opening line:

> This is synthetic data only. I built it to demonstrate the engineering pattern
> behind trustworthy People Analytics, not to represent real employee data.
