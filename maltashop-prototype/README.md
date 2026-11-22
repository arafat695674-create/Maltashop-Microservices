# MaltaShop "place an order" prototype

Unit 5 (SOA and Microservices Fundamentals) prototype. Four independent services implement the **place-order flow**
of MaltaShop Ltd as an **orchestrated saga** over RabbitMQ, with no distributed database transaction.

| Service | Port | Owns (its own PostgreSQL database) | Role |
|---|---|---|---|
| `order-service` | 8001 | orders, order items, idempotency keys, outbox | REST API (`POST /v1/orders`) and saga orchestrator |
| `inventory-service` | 8002 | stock, reservations | Reserves stock; compensating `ReleaseStock` |
| `payment-service` | - | payments | Charges the (simulated) external provider |
| `notification-service` | 8004 | notification log | Consumes order events and "sends" notifications |
| `rabbitmq` | 15672 (UI) | broker state | Topic exchange `maltashop`, durable queues, dead-letter queues |

## How it works
`POST /v1/orders` (with an `Idempotency-Key`) writes the order and outbox rows in one local transaction and returns `202`.
The Order service then drives the saga with commands (`ReserveStock`, `ChargePayment`, `ReleaseStock`) and reacts to replies
(`StockReserved`, `PaymentSucceeded`/`PaymentFailed`, `StockReleased`). A declined card triggers the compensating action
(stock released, order `CANCELLED`). Consumers are idempotent (message IDs recorded in the same transaction as the state change),
failed messages go to `<queue>.dlq`, and one correlation ID (`X-Correlation-ID`) appears in every service's JSON logs.

## Run it
Requires Docker with the Compose plugin.
```bash
cp .env.example .env              # development credentials; edit if you wish
docker compose up --build         # starts 4 services, 4 databases and RabbitMQ
python scripts/smoke_test.py      # in a second terminal (standard library only): 3 end-to-end scenarios
docker compose down -v            # stop and delete volumes
```
Try it by hand:
```bash
curl -i -X POST http://localhost:8001/v1/orders -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-1" -H "X-Correlation-ID: demo-corr-1" \
  -d '{"customer_id":"cust-42","payment_token":"tok_ok","items":[{"sku":"TV-55","qty":1,"unit_price":499.0}]}'
curl http://localhost:8001/v1/orders/<order_id>            # PENDING -> CONFIRMED
curl http://localhost:8004/v1/notifications                # events consumed by the second service
docker compose logs | grep demo-corr-1                     # same correlation ID in all four services
```
Use `payment_token: "tok_decline"` to see the compensation path. Seed stock: `TV-55`=10, `KETTLE-1`=50, `LAPTOP-14`=5.

## Environment variables
| Variable | Used by | Meaning |
|---|---|---|
| `RABBITMQ_USER`, `RABBITMQ_PASS` | compose, all services | Broker credentials (compose builds `RABBITMQ_URL` from them) |
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | compose, all services | Credentials for the four PostgreSQL databases |
| `LOG_LEVEL` | all services | `INFO` by default |
| `SERVICE_NAME` | all services | Set per service by compose; written into every log line |
| `DATABASE_URL` | all services | Own database DSN (set per service by compose) |
| `RABBITMQ_URL` | all services | AMQP URL (set by compose) |

## Tests
```bash
cd order-service && pip install -r requirements.txt -r requirements-dev.txt
DATABASE_URL=unused RABBITMQ_URL=unused SERVICE_NAME=order-service python -m pytest -q   # same for payment-service
```
Includes the saga state-machine tests and the consumer-driven contract test between Order and Payment
(`contracts/charge_payment.schema.json`, copied into each service's `tests/`). CI: `.github/workflows/ci.yml`.

## Limitations
Payment provider is simulated; no gateway/authentication/TLS; one broker node; no metrics or tracing; no saga timeout.
See report Section 6.3.

---
*Reconstructed in session 3: the original README was overwritten by mistake. Content was rebuilt from the code, `docker-compose.yml`
and the report; check it against your own run before submission.*
