# Case Note: TXN-3012209 (resolved, false positive)

Flagged for a transaction 4.2x the card's average amount. Investigation
found the account had two prior large purchases in the same category
(electronics) six months apart, and the shipping address matched the
cardholder's registered address on file. Cardholder confirmed the purchase
by phone. Root cause: model does not yet account for seasonal purchase
patterns (this occurred during a holiday sale). Recommended follow-up:
add a seasonality feature to reduce false positive rate on legitimate large
purchases.
