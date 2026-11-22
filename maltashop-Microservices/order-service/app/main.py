"""Order service: REST API + saga orchestrator + transactional outbox relay. Owns orders_db."""
import asyncio, json, os, uuid
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field
from .common import (connect, consume, correlation_id, declare_queue, log, publish, wait_for_db)
from .saga import next_step

DSN = os.environ["DATABASE_URL"]
AMQP = os.environ["RABBITMQ_URL"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
  id uuid PRIMARY KEY, idempotency_key text UNIQUE NOT NULL, customer_id text NOT NULL,
  total numeric(12,2) NOT NULL, payment_token text NOT NULL, status text NOT NULL, created_at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS order_items (order_id uuid REFERENCES orders(id), sku text, qty int, unit_price numeric(12,2));
CREATE TABLE IF NOT EXISTS outbox (id bigserial PRIMARY KEY, message_id uuid NOT NULL, routing_key text NOT NULL,
  payload jsonb NOT NULL, correlation_id text NOT NULL, published boolean DEFAULT false);
CREATE TABLE IF NOT EXISTS processed_messages (message_id text PRIMARY KEY);   -- inbox: idempotent consumer
"""

class Item(BaseModel):
    sku: str
    qty: int = Field(gt=0)
    unit_price: float = Field(gt=0)

class OrderIn(BaseModel):
    customer_id: str
    items: list[Item] = Field(min_length=1)
    payment_token: str        # opaque token from the external payment provider: no card data is ever stored

state = {"channel": None, "ready": False}

async def add_outbox(cur, routing_key: str, payload: dict):
    await cur.execute("INSERT INTO outbox (message_id, routing_key, payload, correlation_id) VALUES (%s,%s,%s,%s)",
                      (uuid.uuid4(), routing_key, json.dumps(payload), correlation_id.get()))

# ---------- Outbox relay: publishes rows written in the SAME transaction as the state change ----------
async def relay_outbox():
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row) as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT * FROM outbox WHERE NOT published ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED")
                    for row in await cur.fetchall():
                        await publish(state["channel"], row["routing_key"], row["payload"], str(row["message_id"]), row["correlation_id"])
                        await cur.execute("UPDATE outbox SET published = true WHERE id = %s", (row["id"],))
                        token = correlation_id.set(row["correlation_id"])
                        log("INFO", "event published", routing_key=row["routing_key"], order_id=row["payload"].get("order_id"))
                        correlation_id.reset(token)
        except Exception as exc:        # broker down: rows stay unpublished and are retried (at-least-once)
            log("ERROR", "outbox relay error", error=str(exc))
        await asyncio.sleep(0.5)

# ---------- Saga orchestrator: reacts to replies from Inventory and Payment ----------
async def on_reply(body: dict, message_id: str, routing_key: str):
    async with await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row) as conn:
        async with conn.transaction():
            cur = conn.cursor()
            # Idempotent consumer: a redelivered message is detected by its ID and skipped.
            await cur.execute("INSERT INTO processed_messages VALUES (%s) ON CONFLICT DO NOTHING", (message_id,))
            if cur.rowcount == 0:
                log("INFO", "duplicate reply ignored", message_id=message_id); return
            await cur.execute("SELECT * FROM orders WHERE id = %s FOR UPDATE", (body["order_id"],))
            order = await cur.fetchone()
            step = next_step(order["status"], body["type"]) if order else None
            if step is None:
                log("WARN", "reply does not fit order state, ignored", status=order and order["status"], reply=body["type"]); return
            new_status, actions = step
            await cur.execute("UPDATE orders SET status = %s WHERE id = %s", (new_status, order["id"]))
            for key, mtype in actions:
                payload = {"type": mtype, "order_id": str(order["id"]), "customer_id": order["customer_id"], "total": float(order["total"])}
                if mtype == "ChargePayment":
                    payload["payment_token"] = order["payment_token"]
                await add_outbox(cur, key, payload)          # state change + next command commit atomically
            log("INFO", "saga transition", order_id=body["order_id"], reply=body["type"], from_status=order["status"], to_status=new_status)

@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = await wait_for_db(DSN)
    async with conn:
        await conn.execute(SCHEMA); await conn.commit()
    broker = await connect(AMQP)
    state["channel"] = await broker.channel()
    q = await declare_queue(state["channel"], "order.replies", ["reply.order"])
    await consume(q, on_reply)
    tasks = [asyncio.create_task(relay_outbox())]
    state["ready"] = True
    log("INFO", "order service started")
    yield
    for t in tasks: t.cancel()
    await broker.close()

app = FastAPI(title="Order service", lifespan=lifespan)

@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    token = correlation_id.set(cid)
    try:
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = cid
        return response
    finally:
        correlation_id.reset(token)

@app.get("/healthz")                     # liveness: process only, never external dependencies
async def healthz(): return {"status": "alive"}

@app.get("/readyz")                      # readiness: can we take traffic?
async def readyz(response: Response):
    try:
        async with await psycopg.AsyncConnection.connect(DSN) as c: await c.execute("SELECT 1")
        ok = state["ready"] and not state["channel"].is_closed
    except Exception: ok = False
    if not ok: response.status_code = 503
    return {"status": "ready" if ok else "not ready"}

@app.post("/v1/orders", status_code=202)
async def place_order(order: OrderIn, response: Response, idempotency_key: str = Header(default=None)):
    if not idempotency_key:
        raise HTTPException(400, {"code": "IDEMPOTENCY_KEY_REQUIRED", "message": "Idempotency-Key header is required"})
    total = round(sum(i.qty * i.unit_price for i in order.items), 2)
    order_id = uuid.uuid4()
    async with await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row) as conn:
        try:
            async with conn.transaction():                     # ONE local ACID transaction: order + items + outbox rows
                cur = conn.cursor()
                await cur.execute("INSERT INTO orders (id, idempotency_key, customer_id, total, payment_token, status) VALUES (%s,%s,%s,%s,%s,'PENDING')",
                                  (order_id, idempotency_key, order.customer_id, total, order.payment_token))
                for i in order.items:
                    await cur.execute("INSERT INTO order_items VALUES (%s,%s,%s,%s)", (order_id, i.sku, i.qty, i.unit_price))
                base = {"order_id": str(order_id), "customer_id": order.customer_id, "total": total,
                        "items": [i.model_dump() for i in order.items]}
                await add_outbox(cur, "evt.order.placed", {**base, "type": "OrderPlaced"})      # business event
                await add_outbox(cur, "cmd.inventory.reserve", {**base, "type": "ReserveStock"}) # saga step 1
        except psycopg.errors.UniqueViolation:                 # retry with same key: return the original order, never a duplicate
            cur = conn.cursor()
            await cur.execute("SELECT id, status FROM orders WHERE idempotency_key = %s", (idempotency_key,))
            existing = await cur.fetchone()
            response.status_code = 200
            log("INFO", "idempotent replay", order_id=str(existing["id"]))
            return {"order_id": str(existing["id"]), "status": existing["status"], "replayed": True}
    log("INFO", "order accepted", order_id=str(order_id), total=total)
    return {"order_id": str(order_id), "status": "PENDING", "replayed": False}

@app.get("/v1/orders/{order_id}")
async def get_order(order_id: uuid.UUID):
    async with await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row) as conn:
        cur = conn.cursor()
        await cur.execute("SELECT id, customer_id, total, status FROM orders WHERE id = %s", (order_id,))
        row = await cur.fetchone()
    if not row: raise HTTPException(404, {"code": "ORDER_NOT_FOUND", "message": "No such order"})
    row["id"] = str(row["id"]); row["total"] = float(row["total"])
    return row
