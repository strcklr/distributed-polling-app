# 2. Transactional outbox for poll events

Status: accepted (2026-11-01)

## Context

M3 emails participants when a poll is created and when it closes. The email must go out if and only if the state change happened, and each participant must get each email exactly once, even if the API or the worker crashes.

Sending email directly from the API handler can't meet that: the poll write and the send can't be made atomic, so a crash between them either loses the email or sends it for a change that never committed. The outbox pattern fixes this by recording the event in the same atomic write as the state change, and having a separate worker deliver it.

Constraints carried over from [ADR 0001](0001-dynamodb-table-design.md):

- A vote is a single conditional `UpdateItem` whose `ALL_NEW` result tells exactly one caller that it cast the last vote.
- The close is a conditional `OPEN -> CLOSED` update that succeeds once.
- A poll is reported as `CLOSED` once every participant has voted, whatever its stored status.

## Decision

### Events

Two event types, each emitted at most once per poll:

| Event | ID | Written with |
|---|---|---|
| `POLL_CREATED` | `<pollID>#created` | `Store.Create` |
| `POLL_CLOSED` | `<pollID>#closed` | the close after the last vote |

Votes don't emit events.

The ID is deterministic, so a retried create or close can't produce a second event, and it gives the email dedup (#16) a natural key: `(eventId, recipient)`.

Each event carries a snapshot of what the email needs, taken in the same write: the question, options and recipients, plus the final counts for `POLL_CLOSED`. The worker never reads the poll, so it emails the state as of the event rather than whatever the poll looks like later. A closed poll's counts are already final, since every participant has voted.

### Participants are email addresses

Participants become email addresses, used both as the voter's identity and as the recipient. `poll.New` validates each one with `net/mail.ParseAddress`, accepts only a bare address (no display name), and lowercases it so that `Alice@example.com` and `alice@example.com` are the same voter. `votes.#p` already handles the dots and `@` in a map key.

### A separate `Outbox` table

Table `Outbox`, partition key `eventId` (string):

| Attribute | Type | Notes |
|---|---|---|
| `eventId` | S | `<pollID>#created` or `<pollID>#closed` |
| `type` | S | `POLL_CREATED` or `POLL_CLOSED` |
| `pollId` | S | |
| `occurredAt` | S | RFC 3339 timestamp |
| `payload` | M | The snapshot described above |
| `pending` | S | Set to `"1"` when written; removed once delivered |
| `leaseUntil` | N | When the current claim expires, in Unix milliseconds |
| `claimedBy` | S | The worker that holds the claim |

A global secondary index `pending-index` (partition key `pending`, sort key `occurredAt`) lists undelivered events oldest first. It's a sparse index: an item without a `pending` attribute isn't in it, so removing `pending` takes a delivered event out of the index, like a partial index on `WHERE NOT processed` in Postgres. Delivered events stay in the table as a record.

`TransactWriteItems` can span tables, so keeping events in their own table costs nothing in atomicity, and a DynamoDB Stream on `Outbox` would carry only events.

### Writes

**Create** becomes one `TransactWriteItems` call:

1. `Put` the poll, condition `attribute_not_exists(id)`.
2. `Put` the `POLL_CREATED` event, condition `attribute_not_exists(eventId)`.

**Close** becomes one `TransactWriteItems` call, sent by the caller whose vote's `ALL_NEW` shows the full count:

1. `Update` the poll: `SET #status = :closed`, condition `#status = :open`.
2. `Put` the `POLL_CLOSED` event, condition `attribute_not_exists(eventId)`.

The event's condition can't fail while the poll's condition holds, since each event ID is written once with its state change. It's there so a bug elsewhere fails loudly instead of overwriting an event.

**Vote** stays a plain `UpdateItem` with `ALL_NEW`. It can't become a transaction: a transaction item can only return the old item when its condition fails, never the item as the write left it, so no voter would learn that its vote was the last. A transaction also can't operate on the same item twice, so the vote and the close can't share one.

A failed condition inside a transaction surfaces as `TransactionCanceledException`, with one `CancellationReason` per item, in order. The store reads the poll item's reason:

- Create: `ConditionalCheckFailed` on item 1 means `ErrAlreadyExists`.
- Close: `ConditionalCheckFailed` on item 1 means the poll is already closed, which counts as success, as in ADR 0001.
- Anything else is returned as an error.

### Reading the outbox: polling with leases

The worker (#14) polls `pending-index`, then claims each event before delivering it:

```
UpdateExpression:    SET leaseUntil = :until, claimedBy = :me
ConditionExpression: attribute_exists(pending)
                     AND (attribute_not_exists(leaseUntil) OR leaseUntil < :now)
```

- Only one worker holds an event at a time. If it crashes, the lease expires and another worker claims the event again.
- When every recipient has been handled, the worker sends `REMOVE pending, leaseUntil, claimedBy`, which takes the event out of the index.
- A re-claimed event is delivered again from the start. The outbox therefore guarantees **at least once** delivery. #16 records each `(eventId, recipient)` send so a re-delivery skips recipients already emailed. That narrows duplicates to one window it can't remove: a crash after the SMTP send but before the record is written. Neither order fixes this on its own, since recording first turns the same crash into a lost email. #16 decides how to handle it, for example a `Message-ID` derived from `(eventId, recipient)` so the receiving side can recognize a repeat.
- Index reads are eventually consistent, so a new event can take a moment to appear, and an event already delivered can briefly still be listed. The claim's `attribute_exists(pending)` condition rejects the second case.

Streams can later replace polling as the way the worker learns of new events. The table, the writes and the dedup don't change.

### The crash between the last vote and the close

ADR 0001 left this gap cosmetic: the stored status stays `OPEN`, but the store reports the poll as `CLOSED`. With the outbox, the same crash also means `POLL_CLOSED` is never written, so nobody is emailed.

- **Now: read repair.** When `Get`, or a `CastVote` that fails its condition, sees a poll where every participant has voted but the stored status is `OPEN`, the store sends the close transaction. The close is idempotent, so concurrent repairs are safe. This gives a `GET` a write side effect, which is acceptable because the write is idempotent and only happens once per poll.
- **M4: the deadline.** Every poll gets a `closesAt`, and the scheduler closes due polls through the same close transaction. A stuck poll that nobody reads is then closed, and its email sent, by its deadline.

## Alternatives considered

- **Change data capture instead of an outbox.** Enable a Stream on `Polls`, and detect `OPEN -> CLOSED` by comparing old and new images. No transactions are needed, and the event can't disagree with the state, because the state change *is* the event. Rejected because the point of M3 is to learn the outbox pattern, because consumers would be coupled to the `Polls` schema and see every vote, and because the stream keeps records for only 24 hours. It has the same crash gap, since a close that's never written emits no event.
- **Outbox items in the `Polls` table.** One table, but finding pending events needs an index anyway, and a stream on `Polls` would mix events with every vote.
- **DynamoDB Streams from the start.** Simpler for the worker, but polling with leases teaches the claim pattern M4's scheduler needs too. Streams can replace polling later without changing the writes.
- **The vote as a transaction** (vote update plus the close, or plus an event). Not possible without losing `ALL_NEW`, as described above. Votes don't need events.
- **A sweeper for fully voted `OPEN` polls.** Finding them needs another index or a scan. Read repair plus M4's deadline covers the gap with less machinery.
- **Participants as `{name, email}`.** Clearer for a UI, but changes the model, the `votes` keys, the condition expression, the API and the tests, for little gain before M5.

## Consequences

- **Cost:** transactional writes consume twice the write capacity of ordinary writes for each item. Create and close go from one write to two items at double cost. Votes, the common write, are unchanged.
- **One hot index partition:** every pending event shares the `pending = "1"` index key. That's far within limits at this scale. A busier system would shard the key (`"1".."N"`) and have workers poll every shard.
- **Clock skew:** leases compare timestamps from different workers. The lease duration has to be much longer than any expected skew.
- **The crash gap closes fully only in M4.** Until then, a fully voted poll that nobody reads again stays stored as `OPEN` and isn't emailed.
- **`MemoryStore` doesn't record events.** The outbox exists only in DynamoDB. `MemoryStore` stays for the API's unit tests.
- **Local development:** `cmd/createtable` also creates `Outbox` and `pending-index`, and the server reads its name from `OUTBOX_TABLE`.
- **ADR 0001 correction:** ADR 0001 expected the vote to become a transaction in M3. Only create and close do.
- **Open issue: voter impersonation.** A participant is identified only by an address in the request, so anyone who knows an address can vote as that person. That was already true with names, and M3 doesn't change it. M5's per-participant voting links are expected to fix it; to be discussed before then.
