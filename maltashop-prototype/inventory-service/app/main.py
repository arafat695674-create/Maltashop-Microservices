"""Inventory service: reserves and releases stock on command. Owns inventory_db."""
import json, os, uuid
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Request, Response
from .common import connect, consume, correlation_id, declare_queue, log, publish, wait_for_db

DSN = os.environ["DATABASE_URL"]
AMQP = os.environ["RABBITMQ_URL"]
state = {"channel": None, "ready": False}

async def reply(_unused, order_id: str, rtype: str, **extra):
    """Reply goes out after the local commit. Because handlers are naturally idempotent, a redelivered
    command (e.g. crash between commit and ack) simply re-sends the same reply."""
    await publish(state["channel"], "reply.order", {"type": rtype, "order_id": order_id, **extra}, str(uuid.uuid4()), correlation_id.get())

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (sku text PRIMARY KEY, available int NOT NULL CHECK (available >= 0));
CREATE TABLE IF NOT EXISTS reservations (order_id text PRIMARY KEY, lines jsonb NOT NULL, status text NOT NULL);
INSERT INTO stock VALUES ('TV-55', 10), ('KETTLE-1', 50), ('LAPTOP-14', 5) ON CONFLICT DO NOTHING;
"""

async def on_command(body: dict, message_id: str, routing_key: str):
    oid = body["order_id"]
    outcome = None
    async with await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row) as conn:
        async with conn.transaction():
            cur = conn.cursor()
            if body["type"] == "ReserveStock":
                await cur.execute("SELECT status FROM reservations WHERE order_id = %s", (oid,))
                if await cur.fetchone():
                    outcome = "StockReserved"                  # natural idempotency: reservation for this order already exists
                else:
                    try:
                        async with conn.transaction():         # savepoint: all lines or nothing
                            for line in body["items"]:
                                await cur.execute("UPDATE stock SET available = available - %s WHERE sku = %s", (line["qty"], line["sku"]))
                                if cur.rowcount == 0: raise psycopg.errors.CheckViolation("unknown sku")
                            await cur.execute("INSERT INTO reservations VALUES (%s,%s,'RESERVED')", (oid, json.dumps(body["items"])))
                        outcome = "StockReserved"
                    except psycopg.errors.CheckViolation:      # CHECK (available >= 0) protects against overselling
                        outcome = "StockRejected"
            elif body["type"] == "ReleaseStock":               # compensating action
                await cur.execute("SELECT lines, status FROM reservations WHERE order_id = %s FOR UPDATE", (oid,))
                r = await cur.fetchone()
                if r and r["status"] == "RESERVED":
                    for line in r["lines"]:
                        await cur.execute("UPDATE stock SET available = available + %s WHERE sku = %s", (line["qty"], line["sku"]))
                    await cur.execute("UPDATE reservations SET status = 'RELEASED' WHERE order_id = %s", (oid,))
                outcome = "StockReleased"
    log("INFO", "inventory command handled", order_id=oid, command=body["type"], outcome=outcome)
    await reply(None, oid, outcome)

@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = await wait_for_db(DSN)
    async with conn:
        await conn.execute(SCHEMA); await conn.commit()
    broker = await connect(AMQP)
    state["channel"] = await broker.channel()
    q = await declare_queue(state["channel"], "inventory.commands", ["cmd.inventory.*"])
    await consume(q, on_command)
    state["ready"] = True
    log("INFO", "inventory service started")
    yield
    await broker.close()

app = FastAPI(title="Inventory service", lifespan=lifespan)

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

@app.get("/v1/stock/{sku}")
async def get_stock(sku: str):
    async with await psycopg.AsyncConnection.connect(DSN, row_factory=dict_row) as conn:
        cur = conn.cursor(); await cur.execute("SELECT sku, available FROM stock WHERE sku = %s", (sku,)); row = await cur.fetchone()
    if not row: raise HTTPException(404, {"code": "SKU_NOT_FOUND", "message": "Unknown SKU"})
    return row
