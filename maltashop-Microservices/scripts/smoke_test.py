"""End-to-end smoke test (standard library only). Run after `docker compose up`.
Scenario A: happy path -> CONFIRMED + notification consumed.
Scenario B: declined card -> saga compensates -> CANCELLED, stock restored.
Scenario C: same Idempotency-Key twice -> one order only."""
import json, sys, time, urllib.request, uuid

ORDER, INVENTORY, NOTIF = "http://localhost:8001", "http://localhost:8002", "http://localhost:8004"

def call(method, url, body=None, headers=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, json.loads(r.read()), r.headers

def wait_status(order_id, wanted, timeout=15):
    end = time.time() + timeout
    while time.time() < end:
        status = call("GET", f"{ORDER}/v1/orders/{order_id}")[1]["status"]
        if status in wanted: return status
        time.sleep(0.5)
    raise AssertionError(f"order {order_id} stuck in {status}")

def place(token, qty=1, key=None, cid=None):
    body = {"customer_id": "cust-42", "payment_token": token, "items": [{"sku": "TV-55", "qty": qty, "unit_price": 499.0}]}
    return call("POST", f"{ORDER}/v1/orders", body, {"Idempotency-Key": key or str(uuid.uuid4()), "X-Correlation-ID": cid or str(uuid.uuid4())})

stock0 = call("GET", f"{INVENTORY}/v1/stock/TV-55")[1]["available"]

cid = str(uuid.uuid4()); status, o, h = place("tok_ok", cid=cid)
print("A  POST /v1/orders ->", status, o, "correlation:", h.get("X-Correlation-ID"))
assert wait_status(o["order_id"], {"CONFIRMED"}) == "CONFIRMED"
time.sleep(1)
kinds = [n["kind"] for n in call("GET", f"{NOTIF}/v1/notifications?order_id={o['order_id']}")[1]["notifications"]]
print("A  notifications consumed by notification-service:", kinds)
assert "OrderPlaced" in kinds and "OrderConfirmed" in kinds
assert call("GET", f"{INVENTORY}/v1/stock/TV-55")[1]["available"] == stock0 - 1

stock1 = call("GET", f"{INVENTORY}/v1/stock/TV-55")[1]["available"]
status, o2, _ = place("tok_decline", qty=2)
assert wait_status(o2["order_id"], {"CANCELLED"}) == "CANCELLED"
time.sleep(1)
print("B  declined card -> CANCELLED; stock restored:", call("GET", f"{INVENTORY}/v1/stock/TV-55")[1]["available"] == stock1)
assert call("GET", f"{INVENTORY}/v1/stock/TV-55")[1]["available"] == stock1

key = str(uuid.uuid4()); s1, a, _ = place("tok_ok", key=key); s2, b, _ = place("tok_ok", key=key)
print("C  same Idempotency-Key twice ->", s1, s2, "same order:", a["order_id"] == b["order_id"])
assert s2 == 200 and a["order_id"] == b["order_id"]
print("ALL SCENARIOS PASSED"); sys.exit(0)
