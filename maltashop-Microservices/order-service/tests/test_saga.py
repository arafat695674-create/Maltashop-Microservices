import json, pathlib
from app.saga import next_step

def test_happy_path():
    assert next_step("PENDING", "StockReserved") == ("STOCK_RESERVED", [("cmd.payment.charge", "ChargePayment")])
    assert next_step("STOCK_RESERVED", "PaymentSucceeded")[0] == "CONFIRMED"

def test_payment_failure_triggers_compensation():
    status, actions = next_step("STOCK_RESERVED", "PaymentFailed")
    assert status == "COMPENSATING" and actions == [("cmd.inventory.release", "ReleaseStock")]
    assert next_step("COMPENSATING", "StockReleased")[0] == "CANCELLED"

def test_duplicate_or_out_of_order_reply_is_ignored():
    assert next_step("CONFIRMED", "PaymentSucceeded") is None
    assert next_step("PENDING", "PaymentSucceeded") is None

def test_charge_payment_contract():
    """Contract test (producer side): the command Order builds must satisfy the schema Payment relies on."""
    import jsonschema
    schema = json.loads((pathlib.Path(__file__).parent / "charge_payment.schema.json").read_text())
    cmd = {"type": "ChargePayment", "order_id": "o-1", "customer_id": "c-1", "total": 99.5, "payment_token": "tok_ok"}
    jsonschema.validate(cmd, schema)
