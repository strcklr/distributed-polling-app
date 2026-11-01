# 1. DynamoDB table design

Status: accepted (2026-10-31)

## Context

M2 replaces `MemoryStore` with a DynamoDB-backed `Store`. The design has to support these access patterns:

| # | Access pattern | Used by |
|---|---|---|
| 1 | Create a poll | `Store.Create` |
| 2 | Get a poll by ID, with its vote counts | `Store.Get` |
| 3 | Record a vote at most once per participant | `Store.CastVote` |
| 4 | Count votes per option | `Store.Get`, `Store.CastVote` |
| 5 | Close the poll exactly once when the last participant votes | `Store.CastVote` |

Constraints:

- Two concurrent `CastVote` calls must never both count, and the poll must never be closed twice or left open after the last vote.
- `Store.CastVote` returns the updated poll, including counts and status.
- Many participants may vote on the same poll at the same moment. Issue #12 tests 50 concurrent voters, and every vote must succeed.
- Participants are listed when a poll is created, so a poll's size is bounded and known up front.

## Decision

### One table, one item per poll

Table `Polls`, partition key `id` (string), no sort key. Each poll, including its votes, is a single item:

| Attribute | Type | Notes |
|---|---|---|
| `id` | S | `poll.Poll.ID` |
| `question` | S | |
| `options` | L of S | Order matters: votes refer to options by index. |
| `participants` | L of S | |
| `status` | S | `OPEN` or `CLOSED`. A reserved word, so always written as `#status`. |
| `votes` | M of participant → N | Option index each participant chose. Created as an empty map. |
| `counts` | L of N | One counter per option, created as zeros. |
| `votesCast` | N | Number of entries in `votes`. |

All of a poll's invariants involve the poll and its votes together. Keeping them in one item makes each check a single-item conditional write, which DynamoDB applies atomically without a transaction.

### Access patterns

| # | Operation |
|---|---|
| 1 | `PutItem` with `attribute_not_exists(id)`, so an ID collision fails instead of overwriting. |
| 2 | `GetItem` with `ConsistentRead: true`, so a client sees its own vote immediately. Counts are stored on the item, so nothing else needs reading. |
| 3, 4 | One `UpdateItem`, described below. |
| 5 | A second, conditional `UpdateItem`, described below. |

### Casting a vote: one conditional update per voter

```
UpdateExpression:    SET votes.#p = :option
                     ADD counts[<option>] :one, votesCast :one
ConditionExpression: attribute_exists(id)
                     AND #status = :open
                     AND contains(participants, :participant)
                     AND size(options) > :option
                     AND attribute_not_exists(votes.#p)
ReturnValues:                        ALL_NEW
ReturnValuesOnConditionCheckFailure: ALL_OLD
```

- The condition only involves this participant's entry. Different voters don't conflict, so concurrent votes all succeed on the first attempt.
- The participant name always goes through `ExpressionAttributeNames` (`#p`), because names can contain dots, spaces or reserved words.
- A list index can't be an expression placeholder. `counts[<option>]` is written into the expression as an integer literal, which is safe because `option` is an `int` and the condition checks its range.
- When the condition fails, `ALL_OLD` returns the item as it was, and the store works out which rule was broken, in the same order as `Poll.Vote`:
  - no item: `ErrNotFound`
  - `status` is `CLOSED`: `ErrClosed`
  - not in `participants`: `ErrNotParticipant`
  - option out of range: `ErrInvalidOption`
  - already in `votes`: `ErrAlreadyVoted`

### Closing: a separate, idempotent update

Each increment of `votesCast` is atomic, so exactly one vote's `ALL_NEW` result has `votesCast` equal to the number of participants. That caller sends:

```
UpdateExpression:    SET #status = :closed
ConditionExpression: #status = :open
```

If that condition fails, the poll is already closed. The store treats that as success, so the close is safe to retry.

### A crash between the vote and the close

The two updates aren't atomic. If the process stops after the last vote but before the close, the stored `status` stays `OPEN`. The invariants still hold: every participant is already in `votes`, so no further vote can pass the condition. To keep what clients see correct, the store reports a poll as `CLOSED` whenever `votesCast` equals the number of participants, whatever the stored `status` says. A later step (the M3 outbox or M4 timers) can repair the stored value, because the close is idempotent.

### Errors and retries

Single-voter updates don't contend with each other, so `CastVote` needs no retry loop for conflicts. Throttling and transient errors use the AWS SDK's standard retryer. If those retries run out, the store returns an error that the API maps to `503 Service Unavailable` with `Retry-After`, since the same request would succeed a moment later. `409` stays reserved for `ErrAlreadyVoted` and `ErrClosed`, which retrying won't fix.

## Alternatives considered

- **Optimistic locking on the whole item** (read, run `Poll.Vote` in Go, put with `version = :old`, retry on conflict). It reuses the domain logic unchanged and closes in the same write. Rejected because every write conflicts with every other: with 50 simultaneous voters only one wins each round, and a single-digit retry budget leaves most of them with errors.
- **A separate item for each vote** (`PK = POLL#<id>`, `SK = VOTE#<participant>`, written with `TransactWriteItems` alongside the poll's counters). Scales to very large polls and keeps each vote as its own record. Rejected because polls here are small and bounded: it costs twice the write capacity per vote, all voters still contend on the shared counters, and closing still needs its own conditional step.
- **Votes as a list lined up with `participants`** instead of a map. Stores each name once instead of twice, so about twice the headroom. Rejected because the gain is small, the condition needs a "not voted" placeholder value, and every vote depends on `participants` never being reordered. The map also matches `Poll`'s `votes map[string]int`.

## Consequences

- **Item size:** DynamoDB items are capped at 400 KB, which bounds a poll to roughly tens of thousands of participants with short names. `poll.New` should enforce an explicit maximum so a large poll fails validation instead of failing the write.
- **One hot item per poll:** a single item handles up to about 1,000 writes per second. That's far above what a poll with a fixed participant list needs.
- **Rules in two places:** the voting rules exist both in `Poll.Vote` and in the condition expression. The store's tests must cover each rule against DynamoDB Local so the two can't drift apart.
- **M3 outbox:** recording an outbox entry in the same write turns the vote into a `TransactWriteItems` call (this update plus an outbox `Put`). The condition expression carries over unchanged. Failures then come back in `CancellationReasons`, one per item in the transaction.
- **Local development:** `compose.yaml` runs DynamoDB Local in memory on port 8000.
