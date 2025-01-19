# link_aggregator

## endpoints

terms as used here:

- "URI": a URI, AT-URI, or DID.
- "JSON path": a dot-separated (and dot-prefixed, for now) path to a field in an atproto record. Arrays are noted by `[]` and cannot contain a specific index.

### `GET /links/count`

The number of backlinks to a URI from a specified collection + json path.

Required URL parameters

- `target` (required): the URI. must be URL-encoded.
  - example: `at%3A%2F%2Fdid%3Aplc%3A57vlzz2egy6eqr4nksacmbht%2Fapp.bsky.feed.post%2F3lg2pgq3gq22b`
- `collection` (required): the source NSID of referring documents to consider.
  - example: `app.bsky.feed.post`
- `path` (required): the JSON path in referring documents to consider.
  - example: `.subject.uri`

cURL Example: Get a count of all bluesky likes for a post

```bash
curl 'http://raspberrypi.local:6789/links/count?target=at%3A%2F%2Fdid%3Aplc%3A57vlzz2egy6eqr4nksacmbht%2Fapp.bsky.feed.post%2F3lg2pgq3gq22b&collection=app.bsky.feed.like&path=.subject.uri'

40
```


some todos

- [x] find links and write them to rocksdb
- [x] handle account active status
- [x] handle account deletion
- [ ] handle account privacy setting? (is this a bsky nsid config and should that matter?)
- [x] move ownership of canonical seq to an owned non-atomic
- [x] custom path for db storage
- [x] api server to look up backlink count
- [ ] other useful endpoints for the api server
  - [ ] show all nisd/path links to target
  - [ ] get backlinking dids
  - [ ] paging for all backlinking dids
  - [ ] get count + most recent dids
  - [ ] get count with any dids from provided set
- [ ] write this readme
- [ ] fix it sometimes getting stuck
  - seems to unstick in my possibly-different repro (letting laptop fall asleep) after a bit.
  - [ ] add a detection for no new links coming in after some period
- [~] handle jetstream restart: don't miss events (currently sketch: rewinds cursor by 1us so we will always double-count at least one event)
  - [ ] especially: figure out what the risk is to rotating to another jetstream server in terms of gap/overlap from a different jetstream instance's cursor
- [x] metrics!
  - [x] event ts lag
- [~] machine resource metrics
  - [x] disk consumption
  - [x] cpu usage
  - [x] mem usage
  - [ ] network?
- [ ] make all storage apis return Result
- [ ] handle all the unwraps
- [ ] deadletter queue of some kind for failed db writes
  - [ ] also for valid json that was rejected?
- [x] get it running on raspi
- [ ] get an estimate of disk usage per day after a few days of running
- [ ] make the did_init check only happen on test config (or remove it)
- [ ] actual error types (thiserror?) for lib-ish code
- [ ] clean up the main readme
- [x] web server metrics
- [ ] tokio metrics?
- [ ] handle shutdown cleanly -- be nice to rocksdb
- [x] add user-agent to jetstream request
- [ ] possibly add tracing stuff also to complement metrics (at least for web reqs)
