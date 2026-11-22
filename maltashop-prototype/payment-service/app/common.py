"""Shared helpers: JSON logging with correlation ID, RabbitMQ topology, publish/consume.
This file is deliberately COPIED into each service (not imported from a shared package),
so services remain independently deployable (SOA principle: autonomy, loose coupling)."""
import asyncio, contextvars, json, os, sys, time, uuid
import aio_pika

SERVICE = os.getenv("SERVICE_NAME", "unknown")
EXCHANGE, DLX = "maltashop", "maltashop.dlx"

# ---- Structured logging (mandatory schema: timestamp, level, service, correlation_id, message) ----
correlation_id = contextvars.ContextVar("correlation_id", default="-")

def log(level: str, message: str, **fields):
    rec = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time()*1000)%1000:03d}Z",
           "level": level, "service": SERVICE, "correlation_id": correlation_id.get(), "message": message}
    rec.update(fields)
    print(json.dumps(rec), file=sys.stdout, flush=True)       # 12-factor: logs are a stdout event stream

# ---- RabbitMQ ----
async def connect(url: str, attempts: int = 30):
    for i in range(attempts):
        try:
            return await aio_pika.connect_robust(url)           # auto-reconnect on network failure
        except Exception as exc:                                # broker may still be starting
            log("WARN", "broker not ready, retrying", attempt=i + 1, error=str(exc))
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ unreachable")

async def declare_queue(channel, queue: str, binding_keys: list[str]):
    """Durable queue + dead-letter queue. A message that raises in a consumer is rejected
    (no requeue) and lands in <queue>.dlq instead of blocking the queue (poison message)."""
    ex = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    dlx = await channel.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)
    dlq = await channel.declare_queue(f"{queue}.dlq", durable=True)
    await dlq.bind(dlx, routing_key=queue)
    q = await channel.declare_queue(queue, durable=True, arguments={
        "x-dead-letter-exchange": DLX, "x-dead-letter-routing-key": queue})
    for key in binding_keys:
        await q.bind(ex, routing_key=key)
    return q

async def publish(channel, routing_key: str, body: dict, message_id: str, corr_id: str):
    ex = await channel.get_exchange(EXCHANGE)
    msg = aio_pika.Message(json.dumps(body).encode(), content_type="application/json",
                           delivery_mode=aio_pika.DeliveryMode.PERSISTENT,    # survive broker restart
                           message_id=message_id, correlation_id=corr_id,
                           headers={"correlation_id": corr_id})
    await ex.publish(msg, routing_key=routing_key)   # channel has publisher confirms enabled by default

async def consume(queue, handler):
    """Manual ack: the message is acknowledged only after the handler (and its DB commit) succeeds.
    On exception: reject without requeue -> dead-letter queue."""
    async def _on(message: aio_pika.IncomingMessage):
        async with message.process(requeue=False, ignore_processed=True):
            token = correlation_id.set(message.correlation_id or (message.headers or {}).get("correlation_id") or str(uuid.uuid4()))
            try:
                await handler(json.loads(message.body), message.message_id, message.routing_key)
            except Exception as exc:
                log("ERROR", "handler failed, message dead-lettered", error=str(exc), routing_key=message.routing_key)
                raise
            finally:
                correlation_id.reset(token)
    await queue.consume(_on)

# ---- Database ----
async def wait_for_db(dsn: str, attempts: int = 30):
    import psycopg
    for i in range(attempts):
        try:
            conn = await psycopg.AsyncConnection.connect(dsn)
            return conn
        except Exception as exc:
            log("WARN", "database not ready, retrying", attempt=i + 1, error=str(exc))
            await asyncio.sleep(2)
    raise RuntimeError("database unreachable")
