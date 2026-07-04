# Case Note: TXN-2987441 (resolved, confirmed fraud)

Card showed 4 transactions under $3 within 6 minutes, then a $340 purchase
14 minutes later. Graph analysis showed the card's billing address was
shared with 11 other cards created in the prior 48 hours - consistent with
a card-testing ring. Confirmed fraud after cardholder contact; card was
already reported stolen 2 days prior. Root cause: compromised card number
via a third-party data breach, not a platform vulnerability.
