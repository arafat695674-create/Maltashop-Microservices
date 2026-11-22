"""Payment service: charges via an EXTERNAL provider (simulated). Owns payments_db.
Only opaque provider tokens are handled; no card number ever enters MaltaShop systems (PCI DSS scope reduction)."""
import os, uuid
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Request, Response
from .common import connect, consume, correlation_id, declare_queue, log, publish, wait_for_db

DSN = os.environ["DATABASE_URL"]
AMQP = os.environ["RABBITMQ_URL"]
state = {"channel": None, "ready": False}

async def reply(_unused, order_id: str, rtype: str, **extra):
    """Reply goes out after the local commit. Because handlers are naturally idempotent, a redelivered
    command (e.g. crash between commit and ack) simply re-sends the same reply."""
    await publish(state["channel"], "reply.order", {"type": rtype, "order_id": order_id, **extra}, str(uuid.uuid4()), correlation_id.get())

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (order_id text PRIMARY KEY, amount numeric(12,2) NOT NULL, status text NOT NULL);
"""

def call_provider(token: str, amount: float) -> bool:
    """Stand-in for the external payment provider API. 'tok_decline' simulates a declined card."""
    return token != "tok_decline"

async def on_command(body: dict, message_id: str, routing_key: str):
    oid = body["order_id"]
    async with await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row) as conn:
        async with conn.transaction():
            cur = conn.cursor()
            await cur.execute("SELECT status FROM payments WHERE order_id = %s", (oid,))
            existing = await cur.fetchone()
            if existing:                                       # never charge twice for the same order
                status = existing["status"]
            else:
                status = "SUCCEEDED" if call_provider(body["payment_token"], body["total"]) else "FAILED"
                await cur.execute("INSERT INTO payments VALUES (%s,%s,%s)", (oid, body["total"], status))
    log("INFO", "payment processed", order_id=oid, status=status)
    await reply(None, oid, "PaymentSucceeded" if status == "SUCCEEDED" else "PaymentFailed")

@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = await wait_for_db(DSN)
    async with conn:
        await conn.execute(SCHEMA); await conn.commit()
    broker = await connect(AMQP)
    state["channel"] = await broker.channel()
    q = await declare_queue(state["channel"], "payment.commands", ["cmd.payment.*"])
    await consume(q, on_command)
    state["ready"] = True
    log("INFO", "payment service started")
    yield
    await broker.close()

app = FastAPI(title="Payment service", lifespan=lifespan)

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

