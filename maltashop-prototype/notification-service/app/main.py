"""Notification service: sends (simulated) customer messages when order events arrive. Owns notifications_db.
It is a pure event consumer: if it is slow or crashes, checkout is unaffected (the March incident cannot recur)."""
import os, uuid
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Request, Response
from .common import connect, consume, correlation_id, declare_queue, log, publish, wait_for_db

DSN = os.environ["DATABASE_URL"]
AMQP = os.environ["RABBITMQ_URL"]
state = {"channel": None, "ready": False}

async def reply(cur_conn, order_id: str, rtype: str, **extra):
    """Reply is sent after the local commit. If the process dies in between, the orchestrator
    re-sends the command only if it is built to; here a redelivered command re-sends the stored result."""
    await publish(state["channel"], "reply.order", {"type": rtype, "order_id": order_id, **extra}, str(uuid.uuid4()), correlation_id.get())

SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (id bigserial PRIMARY KEY, message_id text UNIQUE NOT NULL, order_id text NOT NULL,
  kind text NOT NULL, customer_id text, created_at timestamptz DEFAULT now());
"""
TEMPLATES = {"OrderPlaced": "We have received your order", "OrderConfirmed": "Your order is confirmed", "OrderCancelled": "Your order could not be completed"}

async def on_event(body: dict, message_id: str, routing_key: str):
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        cur = await conn.execute("INSERT INTO notifications (message_id, order_id, kind, customer_id) VALUES (%s,%s,%s,%s) ON CONFLICT (message_id) DO NOTHING",
                                 (message_id, body["order_id"], body["type"], body.get("customer_id")))
        if cur.rowcount == 0:
            log("INFO", "duplicate event ignored", message_id=message_id); return
        await conn.commit()
    log("INFO", "event consumed, email sent (simulated)", order_id=body["order_id"], event=body["type"], text=TEMPLATES.get(body["type"], "Update"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = await wait_for_db(DSN)
    async with conn:
        await conn.execute(SCHEMA); await conn.commit()
    broker = await connect(AMQP)
    state["channel"] = await broker.channel()
    q = await declare_queue(state["channel"], "notification.events", ["evt.order.*"])
    await consume(q, on_event)
    state["ready"] = True
    log("INFO", "notification service started")
    yield
    await broker.close()

app = FastAPI(title="Notification service", lifespan=lifespan)

@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    token = correlation_id.set(cid)
    try:
        response = await call_next(request); response.headers["X-Correlation-ID"] = cid; return response
    finally:
        correlation_id.reset(token)

@app.get("/healthz")
async def healthz(): return {"status": "alive"}

@app.get("/readyz")
async def readyz(response: Response):
    try:
        async with await psycopg.AsyncConnection.connect(DSN) as c: await c.execute("SELECT 1")
        ok = state["ready"] and not state["channel"].is_closed
    except Exception: ok = False
    if not ok: response.status_code = 503
    return {"status": "ready" if ok else "not ready"}

@app.get("/v1/notifications")
async def list_notifications(order_id: str):
    async with await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row) as conn:
        cur = conn.cursor(); await cur.execute("SELECT kind, created_at FROM notifications WHERE order_id = %s ORDER BY id", (order_id,))
        rows = await cur.fetchall()
    return {"order_id": order_id, "notifications": [{"kind": r["kind"], "at": r["created_at"].isoformat()} for r in rows]}
