# Work in progress

## Notes

### Backfilling the network

TID: 13 ASCII characters, lexicographically

In the DB:

1. Account
   id
   did (unique)
   rev (a TID), nullable current seen revision. it is guaranteed that any record older than the rev has already been stored.

2. Follow:
   source
   target

3. Feed
   Owner
   Name
   Description

Plan

1. Read from the firehose. For each entry
   1.1 Handle DID
   1.1.1 Read the DID
   1.1.2 If it was found
   1.1.2.1 If the record does not match follows or feeds, continue with the next entry
   1.1.2.2 Otherwise, check if it currently has a backfiller
   1.1.2.2.1 If it does, sends the record to the backfiller
   1.1.2.2.2 otherwise, sends the record to the DB updater
   1.1.3 If it was not found
   1.1.3.1 Create an account
   1.1.3.2 Create a backfiller (id -> backfiller)
   1.1.3.3 send the record to the backfiller channel

2. Back Filler
   2.1 Obtain the CAR file
   2.1.1 In a single transaction
   2.1.1.1 read the record of follows and write them in the DB
   2.1.1.2 read the record of feeds and write them in the DB
   2.1.1.3 update the account with the revision
   2.2 sends all the records in the buffer to the updater
   2.6 destroys itself

locks: when looking for backfillers, a lock must be set, and then released. This lock must also be set
when reading from the channel and sending to the updater. The lock _must_ be release after the drop of the
backfiller!

3. updater
   3.1 The updater reads a record from the channel
   3.2 The updater has N channels, it sends the record in a way that all records for the same DID go in order to
   the same channel
   3.3 each mini-updater:
   3.3.1 Reads the record
   3.3.2 Reads the did, find the rev
   3.3.3 If the
   generate models

### Diesel

```sh
# cargo install diesel_cli_ext
diesel_ext --model
```
