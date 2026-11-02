# Evidence from real runs

Everything in this folder is written by the pipeline during a real run on the full McAuley Lab files
(`scripts/run_real.sh`). Nothing is typed by hand. Commit the whole folder after the run.

| File | Produced by |
|---|---|
| `raw_files_sha256.txt` | `scripts/download_data.sh`: exact input files |
| `run_manifest_first_category.json` | first run, one category |
| `run_manifest_incremental.json` | second run: only the new category is processed |
| `run_manifest_main.json`, `dq_report_main.json` | full forced run: timings, row counts, detector stats, DQ gate |
| `idempotency_check.json` | no-op incremental rerun + forced rerun with identical table checksums |
| `eval/eval_report.{json,md}` | injected-campaign evaluation, 3 seeds |
| `sql/*.md`, `sql/timings_ms.json` | SQL analytics outputs |
| `dashboard.html`, `figures/*.png` | dashboard |
| `log_*.txt` | console logs of each step |
| `RESUME_NUMBERS.md` | every resume number → evidence file + JSON key |
