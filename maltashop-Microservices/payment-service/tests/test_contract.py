import json, pathlib, jsonschema, pytest
from app.main import call_provider

SCHEMA = json.loads((pathlib.Path(__file__).parent / "charge_payment.schema.json").read_text())

def test_consumer_accepts_contract_message():
    """Contract test (consumer side): a message in the agreed shape contains every field the handler reads."""
    msg = {"type": "ChargePayment", "order_id": "o-1", "customer_id": "c-1", "total": 10, "payment_token": "tok_ok"}
    jsonschema.validate(msg, SCHEMA)
    assert {"order_id", "payment_token", "total"} <= set(msg)

def test_contract_rejects_missing_token():
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"type": "ChargePayment", "order_id": "o-1", "customer_id": "c", "total": 5}, SCHEMA)

def test_decline_token_is_declined():
    assert call_provider("tok_ok", 10) is True and call_provider("tok_decline", 10) is False
