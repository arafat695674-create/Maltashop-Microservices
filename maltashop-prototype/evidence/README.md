# Evidence captured in the AI sandbox (NOT a substitute for your own screenshots)

These files were produced when the four services were run **natively** (uvicorn processes, not containers) against a real
RabbitMQ 3.x broker and a real PostgreSQL 16 server (one database per service, one Postgres server for convenience).
The Docker images could **not** be built in that sandbox (no access to Docker Hub), so the Dockerfiles and
`docker compose up` were validated only with `docker compose config`. Re-run everything yourself with `docker compose up --build`
and take your own screenshots for the report (Sections 3 and 6.3).

| File | What it shows |
|---|---|
| smoke_test_output.txt | scripts/smoke_test.py: happy path, declined card + compensation, duplicate Idempotency-Key |
| correlation_corr-iso.jsonl | One correlation ID (`corr-iso`) in order, inventory, payment and notification logs, sorted by time |

Additional manual checks run in the same session (outputs not saved as files):
* Notification stopped -> order still CONFIRMED -> 2 messages waited in `notification.events` -> delivered after restart.
* Out of stock (qty 99) -> order CANCELLED, stock unchanged (5).
* Same `ChargePayment` command published twice -> exactly one row in `payments`.
* Malformed message -> moved to `payment.commands.dlq` (1 message).
* Observed: after restart, OrderConfirmed was recorded before OrderPlaced (concurrent consumers do not guarantee order).
