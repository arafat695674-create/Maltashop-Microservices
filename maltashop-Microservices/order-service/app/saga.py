"""Pure saga state machine (no I/O) so it is trivially unit-testable.
Orchestration: the Order service tells each participant what to do and reacts to replies.
(status, reply_type) -> (new_status, [(routing_key, message_type)])"""

TRANSITIONS = {
    # happy path
    ("PENDING", "StockReserved"):          ("STOCK_RESERVED", [("cmd.payment.charge", "ChargePayment")]),
    ("STOCK_RESERVED", "PaymentSucceeded"): ("CONFIRMED",      [("evt.order.confirmed", "OrderConfirmed")]),
    # failure paths
    ("PENDING", "StockRejected"):          ("CANCELLED",      [("evt.order.cancelled", "OrderCancelled")]),
    ("STOCK_RESERVED", "PaymentFailed"):   ("COMPENSATING",   [("cmd.inventory.release", "ReleaseStock")]),   # compensating action
    ("COMPENSATING", "StockReleased"):     ("CANCELLED",      [("evt.order.cancelled", "OrderCancelled")]),
}

def next_step(status: str, reply_type: str):
    """Returns None for a reply that does not fit the current state (duplicate or out-of-order): ignore it."""
    return TRANSITIONS.get((status, reply_type))
