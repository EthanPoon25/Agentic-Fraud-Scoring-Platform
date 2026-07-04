# Policy: Card Testing Fraud Pattern

Card testing occurs when a fraudster runs many small, rapid transactions
across the same card to check whether it is still active before attempting
a larger purchase. Indicators include: multiple transactions under $5 within
a short window, transactions across unrelated merchant categories, and a
single card linked to an unusually large connected-component in the
account-relationship graph (many distinct billing addresses or email
domains touching one card in a short period).

Recommended action: flag for review if a card shows 3+ sub-$5 transactions
within 10 minutes AND a graph component size above the 95th percentile.
Escalate to Tier 2 if the pattern repeats across multiple cards sharing an
address or device fingerprint.
