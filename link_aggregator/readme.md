# link_aggregator


major todos

- [x] find links and write them to rocksdb
- [x] handle account active status
- [x] handle account deletion
- [ ] move ownership of canonical seq to an owned non-atomic
- [ ] handle account privacy setting? (is this a bsky nsid config and should that matter?)
- [x] api server to look up backlink count
- [ ] other useful endpoints for the api server
  - [ ] show all nisd/path links to target
  - [ ] get backlinking dids
  - [ ] paging for all backlinking dids
  - [ ] get count + most recent dids
  - [ ] get count with any dids from provided set
- [ ] write this readme
- [ ] fix it sometimes getting stuck
- [ ] handle jetstream restart: don't miss events
  - [ ] especially: figure out what the risk is to rotating to another jetstream server in terms of gap/overlap from a different jetstream instance's cursor
- [ ] metrics!
- [ ] make all storage apis return Result
- [ ] handle all the unwraps
- [ ] deadletter queue of some kind for failed db writes
- [ ] get it running on raspi
- [ ] get an estimate of disk usage per day after a few days of running
- [ ] make the did_init check only happen on test config (or remove it)
