# ShopIL business assistant

Assignment 1 — ETL, continuous streaming and a Hebrew Actor/Critic chat.

## Setup on Windows

Python 3.11 was used for the recorded run. The copied streaming source uses standard-library features available in 3.11; the original course package targets 3.13. Use a separate environment for this submission.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install -e . --no-deps
$env:PYTHONUTF8='1'
.\.venv\Scripts\python.exe prepare.py
.\.venv\Scripts\python.exe experiments.py
.\.venv\Scripts\python.exe verify_batch_edges.py
.\.venv\Scripts\python.exe -m pytest tests -q
```

`prepare.py` runs dlt twice, dbt build, the course exercise checker (stages 1–3), and independent SQL assertions. The DuckDB file is prepared before chat and must remain unchanged while the app runs. For a clean experiment rerun, move `chat_demo.sqlite` and its WAL/SHM companions to a backup directory first. Each processing comparison itself uses fresh temporary databases. Preparation is safe to rerun, although initial-load logs will then describe a reload; preserve the supplied evidence.

## Choose a real model

Both roles call a real language model. Automated contract tests use clearly marked mocks and do not prove model quality. No credentials are included.

Option A: copy `.env.example` to `.env` and configure an OpenAI-compatible model server and key locally. Do not commit `.env`.

Option B: use the same local model as the final recorded run, Qwen2.5 7B Instruct Q4_K_M. It requires about 4.7 GB of downloads and a computer with around 16 GB RAM. Both file hashes are checked against the publisher's metadata:

```powershell
.\setup_local_model.ps1
.\.venv\Scripts\python.exe real_model_experiment.py
```

The local model and its Critic remain fallible; inspect the saved manual verification. CPU responses can take several minutes. A hosted model can be configured through option A without changing the pipeline. The earlier 1.5B model failed semantic checks and is not the final configuration. Diagnostic logs retain these failures separately. Local Critic JSON uses bounded ASCII English feedback to avoid malformed multilingual grammar output; user answers remain Hebrew. Pydantic strictly validates both required fields after generation.

The controller adds deterministic checks for the required paid-order figures, date ranges, missing profit data and Hebrew language. These can only reject an answer; they never override a model rejection with approval. Audit records distinguish the raw model evaluation from the final decision. Feedback is requested in English but local models may still use another ASCII-compatible language; SQL reference values are added to relevant rejection feedback. `requirements-lock.txt` records the full environment used for verification; the optional local inference wheel is installed separately by the setup script.

## Start the chat with continuous ingestion

```powershell
.\.venv\Scripts\python.exe app.py --mode live
# Alternatively, after stopping the first process:
.\.venv\Scripts\python.exe app.py --mode microbatch
```

Open http://127.0.0.1:7860. The synthetic producer runs throughout the session through bounded queues. Both modes call the same `StreamEngine.ingest`; each event commits separately. Monitor checkpoint, watermark, counters, open windows and alerts in the UI; produced/consumed queue metrics print to the console. Stop with Ctrl+C. SQLite checkpoints allow restart without double counting. Do not run two producers against the same database at once.

The runtime supervisor displays running/recovering/stopped status. A SQLite operational error triggers up to three retries, reopening the database and recreating the deterministic source from its durable next offset. Persistent or non-storage errors stop visibly rather than leaving stale counters labeled active. Monitoring uses a read-only SQLite transaction. This was added after an observed disk I/O failure; the underlying storage cause was not established. Tests inject recoverable and persistent errors. For an isolated second preview use `python app.py --port 7861 --stream-db live_verified.sqlite`; never share one live database between two producers.

Try asking about paid historical revenue, follow up about its period, then ask for net profit (expenses are absent). Select a product to include its clean review texts. The system supports numeric and product questions over the fixed allowlisted evidence; it does not execute model-generated SQL.

## Evidence and provenance

- `evidence/batch_report.json`: actual intermediate counts, IDs, product metrics, period and reload equality.
- `evidence/load_*.txt`, `dbt_build.txt`, `course_reviews_check.txt`: actual loader and checker output. The dbt warning about 19 raw order/customer relationship failures is expected dirty source data; `fct_orders` filters invalid orders. Errors must be zero.
- `evidence/stream_input.json`, `stream_modes.json`, `*_trace.json`, `batch_flush.json`, `grace_policies.json`, `reconciliation.json`, `dedupe_replay.json`: exact input, settings and observed stream results.
- `evidence/pytest.txt`: automated assertions including timeout flush with no next arrival, crash/restart, strict schema errors, three rejections and concurrent snapshot isolation.
- `evidence/real_model_audit.jsonl`: actual fixed evidence, drafts, decisions, feedback and final states; `real_model_results.json` summarizes the conversation. Injected faulty draft is explicitly labeled; subsequent Actor and Critic calls are real.
- `Report.pdf`: visually verified three-page Hebrew explanation; `REPORT.md` is its editable source. `architecture.svg` is the rendered diagram; `ARCHITECTURE.md` also renders it on GitHub.
- `evidence/manual_verification.md`: two approved answers checked directly against SQL, plus limitations found during model testing.
- `evidence/simulated_chat_audit.jsonl`: explicitly simulated model decisions for three-rejection and snapshot tests. These are separate from real model evidence.
- `evidence/batch_edge_cases.json`: tag-parent integrity and an isolated new product with no reviews or orders, evaluated using the actual compiled mart SQL.
- `evidence/answer_and_live.jpg`: actual app screenshot showing active ingestion and a saved approved answer with source and snapshot time.
- `evidence/live_ingestion.jpg` and `evidence/answer_with_source.jpg`: separate views of the runtime status/counters and the saved answer with source and captured time.
- `evidence/real_snapshot_isolation.json`, `verification_environment.json`: actual event arrival between questions, verified model hashes and package versions. `verify_submission.py` rechecks the saved real-run evidence without calling a model. `finalize_real_demo.py` explicitly continues a preserved real correction trace after the formatting fix; earlier failed runs remain separate.

Course sources: [ETL repository](https://github.com/paulavrahami/etl-course-demo) at `6788526695bbb20ac40f46a947413fff3545b86a`, [Streaming repository](https://github.com/paulavrahami/streaming-course-demo) at `c27b4fc1b343e5bfca1880ad3c5ae34e97f60b71`.

Copied: synthetic `data/`, base dlt loader, original non-review dbt models/macros/tests, review checker (relocated to `tests/course_check.py`), `src/streaming_lab/` engine and runners, and course core streaming tests. Two course timing assertions were adapted for Windows: the live admission budget increased from 20 ms to 200 ms; the timeout-flush test checks its actual flush reason and source progress instead of assuming a 9 ms minimum delay, because the Windows event-loop clock can fire short timers early. Engine code remains unchanged. Independently completed: review pipeline, review staging/mart/tests, preparation and experiment runners, evidence retrieval, prompts, Actor/Critic controller, extra deterministic checks for key figures/missing data/Hebrew language, Gradio app, chat tests and submission documentation. No instructor review solution was used. Optional LanceDB, Dagster and Kafka extensions are outside the required scope.

The course product IDs are retained, and unknown stream IDs are not joined. Matching IDs do not establish disjoint orders or equal payment semantics, so historical paid revenue and stream event amounts are always separate. All source data here is synthetic teaching data. No Moodle credentials or GitHub tokens are included.
