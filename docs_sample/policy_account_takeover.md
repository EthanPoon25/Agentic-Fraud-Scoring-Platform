# Policy: Account Takeover Pattern

Account takeover typically shows a sudden change in transaction behavior
relative to a card's historical average: a transaction amount several
standard deviations above the account's average, combined with a new email
domain or shipping address never seen before on that account.

Recommended action: flag for review if TransactionAmt exceeds avg_amt by
more than 3x stddev_amt AND the associated email domain does not match
prior transactions. Do not auto-block; route to analyst review, since this
pattern also occurs with legitimate large purchases (e.g. holidays).
