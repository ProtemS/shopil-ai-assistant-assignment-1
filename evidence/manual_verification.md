# Manual review of actual approved answers

These checks were performed against the saved SQL results and the exact frozen evidence in the real model audit. They are distinct from schema validation and from the simulated model tests.

| Actual approved answer | SQL and observed result | Review |
|---|---|---|
| Snapshot `466b0dafe26a8f745b08`: 402 paid valid orders, revenue 97,210.10 ILS | `select count(*), sum(amount) from marts.fct_orders where status='paid'` returns `402`, `97210.10` | Correct count, exact amount, ILS unit, historical paid-order meaning, source and period. Stream money was not added. The deliberately wrong 999/999 draft was rejected; the real Actor corrected it and the real Critic approved the correction. |
| Snapshot `ed01e71737bb731246e6`: historical data period 2026-01-01 through 2026-09-27 | `select min(order_date), max(order_date) from marts.fct_orders` returns `2026-01-01`, `2026-09-27` | Both dates match. The source and snapshot footer identify the database and captured time. The answer is in Hebrew. |

The first question froze checkpoint 13. A unique 300-agorot order arrived after capture, before generation. The next question froze checkpoint 14. The old evidence remained constant throughout rejection and correction; the new evidence includes the additional event. See `real_snapshot_isolation.json` for the direct comparison.

The final missing-data answer, snapshot `c2ecb5617762b03a9161`, states in Hebrew that net profit cannot be calculated because expense and cost data are absent. It does not substitute revenue for profit and does not treat the missing value as zero.

The final local model is Qwen2.5-7B-Instruct Q4_K_M with llama-cpp-python 0.3.36. Both model-file SHA-256 hashes were checked against the publisher's Hugging Face metadata. Actor and Critic are separate prompt roles using the same local model weights. The full raw drafts and evaluations are retained.

Limits found during verification: the earlier 1.5B model confused valid orders with paid orders and once confused revenue with profit. The larger model also produced imprecise feedback and formatting debris in some earlier runs. Those runs remain in diagnostic audit files. The final controller therefore adds SQL-based rejection checks, validates the strict Evaluation schema, removes emoji and trailing internal evidence dumps while preserving raw drafts, and never changes a model rejection into an approval. Feedback is requested in English but a local model may use other ASCII-language wording. Model approval is still not a guarantee for arbitrary questions.

The paid-order correction was recorded before the presentation normalization change and preserved verbatim. The final date and missing-data questions were generated again after that change using `finalize_real_demo.py`. This is explicitly documented rather than presenting separate runs as one uninterrupted execution.
