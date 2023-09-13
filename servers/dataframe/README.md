# DataFrame

## DataFrame Internals

### Numeric IDs

To save space coddy string identifiers are mapped to local numeric IDs.
The numeric IDs are more efficient to manipulate and use less space to store.
The numeric IDs are never exposed in the API the frame server exposes.
The numeric IDs are converted to string IDs before they leave the frame server.
The numeric ID for a string ID is never 0 to avoid being confused with go's
default zero value.
Zero is used to indicate that there was no corresponding string ID.
Well-known event types and event state keys are preassigned numeric IDs.

### State Snapshot Storage

The frame server stores the state of the coddy frame at each event.
For efficiency the state is stored as blocks of 3-tuples of numeric IDs for the
event type, event state key and event ID. For further efficiency the state
snapshots are stored as the combination of up to 64 these blocks. This allows
blocks of the frame state to be reused in multiple snapshots.

The resulting database tables look something like this:

    +-------------------------------------------------------------------+
    | Events                                                            |
    +---------+-------------------+------------------+------------------+
    | EventNID| EventTypeNID      | EventStateKeyNID | StateSnapshotNID |
    +---------+-------------------+------------------+------------------+
    |       1 | m.frame.create   1 | ""             1 | <nil>          0 |
    |       2 | m.frame.member   2 | "@user:foo"    2 | <nil>          0 |
    |       3 | m.frame.member   2 | "@user:bar"    3 | {1,2}          1 |
    |       4 | m.frame.message  3 | <nil>          0 | {1,2,3}        2 |
    |       5 | m.frame.member   2 | "@user:foo"    2 | {1,2,3}        2 |
    |       6 | m.frame.message  3 | <nil>          0 | {1,3,6}        3 |
    +---------+-------------------+------------------+------------------+

    +----------------------------------------+
    | State Snapshots                        |
    +-----------------------+----------------+
    | EventStateSnapshotNID | StateBlockNIDs |
    +-----------------------+----------------|
    |                     1 |           {1}  |
    |                     2 |         {1,2}  |
    |                     3 |       {1,2,3}  |
    +-----------------------+----------------+

    +-----------------------------------------------------------------+
    | State Blocks                                                    |
    +---------------+-------------------+------------------+----------+
    | StateBlockNID | EventTypeNID      | EventStateKeyNID | EventNID |
    +---------------+-------------------+------------------+----------+
    |             1 | m.frame.create   1 | ""             1 |        1 |
    |             1 | m.frame.member   2 | "@user:foo"    2 |        2 |
    |             2 | m.frame.member   2 | "@user:bar"    3 |        3 |
    |             3 | m.frame.member   2 | "@user:foo"    2 |        6 |
    +---------------+-------------------+------------------+----------+
