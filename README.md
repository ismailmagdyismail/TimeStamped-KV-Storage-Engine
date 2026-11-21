# Timestamped KV Storage Engine

An in-memory Rust key-value store for exploring transactional concurrency.

## MVCC + OCC

- Each transaction reads a timestamped MVCC snapshot and keeps its read/write sets.
- Writes are buffered; transactions read their own writes.
- Commit serializes validation, assigns one commit timestamp, then atomically publishes a copy-on-write store.
- OCC rejects stale write/write and read/write dependencies, preventing write skew and competing writes to a previously absent key.
- Old versions remain available to earlier snapshots. There are no range or predicate queries.

## Architecture

![Architecture](docs/arch.png)

## Transaction demos

The test suite demonstrates snapshot reads, conflicts, write skew, phantom writes, atomic commits, and concurrent transactions.

![Transaction demos: phantom writes, write skew, and more](docs/transactions.png)

```sh
cargo test
```

## Missing Features 
1. Not fully serializable: reads of absent keys are never recorded. So "if neither X nor Y exists, create one" lets both transactions read the missing keys and insert different keys. The write-set check only compares each key against itself, so both commit. That is write skew through phantoms.
needs to records missing reads not just discard them

2. Concurrency Control Provided is Stronger Than postgres SSI, less than serializable

3. Read Only Transactions hold commit lock, even though it is not needed

4. Begin_Timestamp is incremented on each new transaction which is not necessary, it needs to be incremented only on Committing. That Global time-stamp could be thought of LOGICAL time / ticks of updates happening to server. Theoretically if no writes stop, that MVCC Control time-stamp could be stable, not change and any upcoming Read-Only transactions can use latest Timestamp without incrementing it. Not increasing it is an indication that database state hasn't changed / time hasn't advanced. on the other hand increasing it means a future database state is available, could be accessed by newer created transactions

5. Deep copying of VersionedStorage could be optimized by replacing VersionedStorage with a concurrent data-structure (Concurrent B-Tree/HashMap). Could be implemented by a Root index (update atomically via CAS | shadow copying), that Root Index contains Multiple Chain(s) (chain for each key), chains are a list of Versions, Timestamps updated atomically by updating an Atomic Head/Tail Ptrs

